package com.adaptris.hpcc;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.validation.Valid;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessageProducerImp;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.hpcc.DfuplusOutputParser.JobStatus;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
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

  @Valid
  @AdvancedConfig
  private TimeInterval monitorInterval;
  @NotBlank
  private String dfuplusCommand;
  @NotBlank
  private String server;
  private String username;
  @InputFieldHint(style = "PASSWORD")
  private String password;

  private transient Calendar nextLogEvent = null;

  public DfuPlusWrapper() {
  }


  @Override
  public void close() {
    // NOP
  }

  @Override
  public void init() throws CoreException {
    // NOP
  }

  @Override
  public void start() throws CoreException {
    // NOP
  }

  @Override
  public void stop() {
    // NOP
  }

  @Override
  public void prepare() throws CoreException {
    // NOP
  }

  public String getDfuplusCommand() {
    return dfuplusCommand;
  }

  public void setDfuplusCommand(String s) {
    this.dfuplusCommand = Args.notBlank(s, "dfuplusCommand");
  }

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = Args.notBlank(server, "server");
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
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

  protected void execute(CommandLine cmdLine) throws ProduceException {
    // Create DFU command
    // String cmd = "dfuplus action=%s format=%s maxrecordsize=%d sourcefile=%s dstname=%s server=%s dstcluster=%s username=%s
    // password=%s overwrite=%d";
    DfuplusOutputParser stdout = new JobSubmissionParser();
    try {
      doExecute(cmdLine, stdout);
      JobStatus status = stdout.getJobStatus();
      long monitorIntervalMs = monitorIntervalMs();
      while (status == JobStatus.NOT_COMPLETE) {
        TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(monitorIntervalMs));
        status = requestStatus(stdout.getWorkUnitId());
        timedLogger("WUID [{}] is [{}]", stdout.getWorkUnitId(), status.name());
      }
      if (status == JobStatus.FAILURE) {
        throw new ProduceException("Job " + stdout.getWorkUnitId() + " was not successful");
      }
    } catch (AbortJobException | InterruptedException e) {
      abortJob(stdout.getWorkUnitId());
    } catch (PasswordException | IOException e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }


  private void doExecute(CommandLine cmdLine, OutputStream stdout) throws ProduceException, AbortJobException {
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
      throw new AbortJobException();
    }
  }


  private JobStatus requestStatus(String wuid) throws ProduceException, AbortJobException, PasswordException, IOException {
    DfuplusOutputParser stdout = new JobStatusParser(wuid);
    CommandLine cmdLine = createQuery(wuid);
    cmdLine.addArgument("action=status");
    doExecute(cmdLine, stdout);
    return stdout.getJobStatus();
  }


  private void abortJob(String wuid) {
    if (isBlank(wuid)) {
      return;
    }
    try {
      CommandLine cmdLine = createQuery(wuid);
      cmdLine.addArgument("action=abort");
      doExecute(cmdLine, new NoOpDfuplusOutputParser());
    } catch (Exception ignored) {
    }
  }

  protected CommandLine createCommand() throws PasswordException, IOException {
    File dfuPlus = validateCmd(getDfuplusCommand());
    CommandLine cmdLine = new CommandLine(dfuPlus.getCanonicalPath());
    cmdLine.addArgument(String.format("server=%s", getServer()));
    if (!isBlank(getUsername())) {
      cmdLine.addArgument(String.format("username=%s", getUsername()));
    }
    if (!isBlank(getPassword())) {
      cmdLine.addArgument(String.format("password=%s", Password.decode(getPassword())));
    }
    return cmdLine;
  }

  private CommandLine createQuery(String wuid) throws PasswordException, IOException {
    CommandLine cmdLine = createCommand();
    if (!isBlank(wuid)) {
      cmdLine.addArgument(String.format("wuid=%s", wuid));
    }
    return cmdLine;
  }

  private File validateCmd(String cmd) throws IOException {
    File dfuPlus = new File(cmd);
    if (dfuPlus.exists() && dfuPlus.isFile() && dfuPlus.canExecute()) {
      return dfuPlus;
    }
    throw new IOException("Can't execute [" + dfuPlus.getCanonicalPath() + "]");
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

  private class AbortJobException extends Exception {
    public AbortJobException() {}
  }
}
