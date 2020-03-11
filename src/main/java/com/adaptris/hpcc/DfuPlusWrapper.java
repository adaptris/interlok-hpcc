/*
 * Copyright 2016 Adaptris Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package com.adaptris.hpcc;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.validation.Valid;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessageProducerImp;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.hpcc.DfuplusOutputParser.JobStatus;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.util.TimeInterval;

/**
 * Base class for {@code dfuplus} based activities.
 * 
 * @author lchan
 *
 */
public abstract class DfuPlusWrapper extends AdaptrisMessageProducerImp {
  private static final TimeInterval MONITOR_INTERVAL = new TimeInterval(30L, TimeUnit.SECONDS);
  // 5 minutes to submit a job...
  private static final TimeInterval EXEC_TIMEOUT_INTERVAL = new TimeInterval(5L, TimeUnit.MINUTES);
  private static final TimeInterval MAX_WAIT = new TimeInterval(1L, TimeUnit.HOURS);

  @Valid
  @AdvancedConfig
  @InputFieldDefault(value = "30 seconds")
  private TimeInterval monitorInterval;

  @Valid
  @AdvancedConfig
  @InputFieldDefault(value = "1 hour")
  private TimeInterval maxWait;

  private transient Calendar nextLogEvent = null;
  private transient Future<JobStatus> currentWorkunit = null;
  protected transient ExecutorService executor;

  public DfuPlusWrapper() {}


  @Override
  public void close() {
    executor.shutdownNow();
    executor = null;
  }

  @Override
  public void init() throws CoreException {
    executor = Executors.newSingleThreadExecutor(new ManagedThreadFactory());
  }

  @Override
  public void start() throws CoreException {
  }

  @Override
  public void stop() {
    if (currentWorkunit != null) {
      currentWorkunit.cancel(true);
    }
    currentWorkunit = null;
  }

  @Override
  public void prepare() throws CoreException {
  }


  /**
   * @return the timeout
   */
  public TimeInterval getMonitorInterval() {
    return monitorInterval;
  }

  /**
   * Set the monitor interval between attempts to query job status.
   * <p>
   * If not specified, then it defaults to 30 seconds
   * </p>
   * 
   * @param t the monitor interval to set, if not specified defaults to 30 seconds.
   */
  public void setMonitorInterval(TimeInterval t) {
    this.monitorInterval = t;
  }

  protected long monitorIntervalMs() {
    return getMonitorInterval() != null ? getMonitorInterval().toMilliseconds() : MONITOR_INTERVAL.toMilliseconds();
  }

  public TimeInterval getMaxWait() {
    return maxWait;
  }

  /**
   * Set the max wait for a workunit to complete.
   * 
   * @param maxWait the max wait, if not specified, defaults to 1 hour.
   */
  public void setMaxWait(TimeInterval maxWait) {
    this.maxWait = maxWait;
  }

  protected long maxWaitMs() {
    return getMaxWait() != null ? getMaxWait().toMilliseconds() : MAX_WAIT.toMilliseconds();
  }

  protected void execute(CommandLine cmdLine) throws ProduceException {
    // Create DFU command
    // String cmd = "dfuplus action=%s format=%s maxrecordsize=%d sourcefile=%s dstname=%s server=%s dstcluster=%s username=%s
    // password=%s overwrite=%d";
    DfuplusOutputParser stdout = new JobSubmissionParser();
    try {
      executeInternal(cmdLine, stdout);
      JobStatus status = stdout.getJobStatus();
      currentWorkunit = executor.submit(new WaitForWorkUnit(status, stdout.getWorkUnitId()));
      status = currentWorkunit.get(maxWaitMs(), TimeUnit.MILLISECONDS);
      if (status == JobStatus.FAILURE) {
        throw new ProduceException("Job " + stdout.getWorkUnitId() + " was not successful");
      }
    }
    catch (AbortJobException | InterruptedException | TimeoutException e) {
      abortJob(stdout.getWorkUnitId());
      throw ExceptionHelper.wrapProduceException(generateExceptionMessage(e), e);
    }
    catch (ExecutionException e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    finally {
      currentWorkunit = null;
    }
  }

  protected void executeInternal(CommandLine cmdLine, OutputStream stdout) throws ProduceException, AbortJobException {
    int exit = -1;
    ExecuteWatchdog watchdog = new ExecuteWatchdog(EXEC_TIMEOUT_INTERVAL.toMilliseconds());
    try (OutputStream out = stdout) {
      Executor cmd = new DefaultExecutor();
      cmd.setWatchdog(watchdog);
      PumpStreamHandler pump = new ManagedPumpStreamHandler(out);
      cmd.setStreamHandler(pump);
      cmd.setExitValues(null);
      exit = cmd.execute(cmdLine);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    if (watchdog.killedProcess() || exit != 0) {
      throw new AbortJobException("Job killed due to timeout/ExitCode != 0");
    }
  }

  protected static String generateExceptionMessage(Exception e) {
    String msg = e.getMessage();
    if (e instanceof InterruptedException) {
      msg = "Interrupted waiting for workunit completion";
    }
    if (e instanceof TimeoutException) {
      msg = "Timeout exceeded for workunit completion";
    }
    return msg;
  }

  protected long calculateWait(long current) {
    long result = TimeUnit.SECONDS.toMillis(1);
    if (current > 0) {
      result = current * 2;
      if (result > monitorIntervalMs()) {
        result = monitorIntervalMs();
      }
    }
    return Math.max(ThreadLocalRandom.current().nextLong(result), current);
  }

  private JobStatus requestStatus(String wuid) throws ProduceException, AbortJobException, PasswordException, IOException {
    DfuplusOutputParser stdout = new JobStatusParser(wuid);
    CommandLine cmdLine = createQuery(wuid);
    cmdLine.addArgument("action=status");
    executeInternal(cmdLine, stdout);
    return stdout.getJobStatus();
  }


  private void abortJob(String wuid) {
    if (isBlank(wuid)) {
      return;
    }
    try {
      CommandLine cmdLine = createQuery(wuid);
      cmdLine.addArgument("action=abort");
      executeInternal(cmdLine, new NoOpDfuplusOutputParser());
    } catch (Exception ignored) {
    }
  }

  private CommandLine createQuery(String wuid) throws PasswordException, IOException {
    CommandLine cmdLine = retrieveConnection(DfuplusConnection.class).createCommand();
    if (!isBlank(wuid)) {
      cmdLine.addArgument(String.format("wuid=%s", wuid));
    }
    return cmdLine;
  }


  private void timedLogger(String text, Object... args) {
    if (nextLogEvent == null) {
      nextLogEvent = Calendar.getInstance();
      nextLogEvent.add(Calendar.MINUTE, -1);
    }
    Calendar now = Calendar.getInstance();
    if (now.getTime().after(nextLogEvent.getTime())) {
      log.trace(text, args);
      nextLogEvent.setTime(now.getTime());
      nextLogEvent.add(Calendar.MINUTE, 5);
    }
  }

  private class NoOpDfuplusOutputParser extends DfuplusOutputParser {

    NoOpDfuplusOutputParser() {
      super();
    }

    @Override
    public JobStatus getJobStatus() {
      return JobStatus.SUCCESS;
    }

    @Override
    protected String getWorkUnitId() {
      return null;
    }

    @Override
    protected void processLine(String line, int logLevel) {}
  }

  private class WaitForWorkUnit implements Callable<JobStatus> {

    private String workUnit;
    private JobStatus status;
    private String threadName;
    
    WaitForWorkUnit(JobStatus initialStatus, String wuid) {
      workUnit = wuid;
      status = initialStatus;
      threadName = Thread.currentThread().getName();
    }

    @Override
    public JobStatus call() throws Exception {
      Thread.currentThread().setName(threadName);
      long totalTime = 0;
      long sleepyTime = calculateWait(0);
      while (status == JobStatus.NOT_COMPLETE) {
        status = requestStatus(workUnit);
        if (status == JobStatus.NOT_COMPLETE) {
          TimeUnit.MILLISECONDS.sleep(sleepyTime);
          totalTime += sleepyTime;
          sleepyTime = calculateWait(sleepyTime);
          if (totalTime >= maxWaitMs()) {
            throw new ProduceException("Internal Timeout has been exceeded");
          }
        }
        timedLogger("WUID [{}]; status=[{}]", workUnit, status.name());
      }
      return status;
    }

  }

}
