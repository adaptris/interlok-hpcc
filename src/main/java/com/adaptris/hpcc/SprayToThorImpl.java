package com.adaptris.hpcc;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.validation.Valid;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
import com.adaptris.util.TimeInterval;
import com.adaptris.util.stream.Slf4jLoggingOutputStream;

public abstract class SprayToThorImpl extends ProduceOnlyProducerImp {

  private static final TimeInterval DEFAULT_TIMEOUT = new TimeInterval(10L, TimeUnit.MINUTES);
  @NotBlank
  private String dfuplusCommand;
  @NotBlank
  private String server;
  @NotBlank
  private String cluster;
  private String username;
  @InputFieldHint(style = "PASSWORD")
  private String password;

  private Boolean overwrite;
  @Valid
  private TimeInterval timeout;

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

  public void setDfuplusCommand(String dfuplusCommand) {
    this.dfuplusCommand = dfuplusCommand;
  }
  
  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
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

  public Boolean getOverwrite() {
    return overwrite;
  }

  public void setOverwrite(Boolean overwrite) {
    this.overwrite = overwrite;
  }
  
  boolean overwrite() {
    return getOverwrite() != null ? getOverwrite().booleanValue() : false;
  }

  /**
   * @return the timeout
   */
  public TimeInterval getTimeout() {
    return timeout;
  }

  /**
   * @param timeout the timeout to set
   */
  public void setTimeout(TimeInterval timeout) {
    this.timeout = timeout;
  }

  protected long timeoutMs() {
    return getTimeout() != null ? getTimeout().toMilliseconds() : DEFAULT_TIMEOUT.toMilliseconds();
  }

  protected void execute(CommandLine cmdLine) throws ProduceException {
    int exit = 0;
    // Create DFU command
    // String cmd = "dfuplus action=%s format=%s maxrecordsize=%d sourcefile=%s dstname=%s server=%s dstcluster=%s username=%s
    // password=%s overwrite=%d";
    try (Slf4jLoggingOutputStream out = new Slf4jLoggingOutputStream(log, "DEBUG")) {
      Executor cmd = new DefaultExecutor();
      ExecuteWatchdog watchdog = new ExecuteWatchdog(timeoutMs());
      cmd.setWatchdog(watchdog);
      PumpStreamHandler pump = new PumpStreamHandler(out);
      cmd.setStreamHandler(pump);
      log.trace("Executing {}", cmdLine);
      exit = cmd.execute(cmdLine);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    if (exit != 0) {
      throw new ProduceException("Spray failed with exit code " + exit);
    }
  }


  protected CommandLine createCommand() throws PasswordException, IOException {
    File dfuPlus = validateCmd(getDfuplusCommand());
    CommandLine cmdLine = new CommandLine(dfuPlus.getCanonicalPath());
    cmdLine.addArgument("action=spray");
    cmdLine.addArgument(String.format("server=%s", getServer()));
    cmdLine.addArgument(String.format("dstcluster=%s", getCluster()));
    if (!isBlank(getUsername())) {
      cmdLine.addArgument(String.format("username=%s", getUsername()));
    }
    if (!isBlank(getPassword())) {
      cmdLine.addArgument(String.format("password=%s", Password.decode(getPassword())));
    }
    cmdLine.addArgument(String.format("overwrite=%d", overwrite() ? 1 : 0));
    return cmdLine;
  }

  private File validateCmd(String cmd) throws IOException {
    File dfuPlus = new File(cmd);
    if (dfuPlus.exists() && dfuPlus.isFile() && dfuPlus.canExecute()) {
      return dfuPlus;
    }
    throw new IOException("Can't execute [" + dfuPlus.getCanonicalPath() + "]");
  }
}
