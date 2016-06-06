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
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.io.output.TeeOutputStream;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessageProducerImp;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
import com.adaptris.util.TimeInterval;
import com.adaptris.util.stream.Slf4jLoggingOutputStream;

/**
 * Base class for {@code dfuplus} based activities.
 * 
 * @author lchan
 *
 */
public abstract class DfuPlusWrapper extends AdaptrisMessageProducerImp {
  private static final TimeInterval DEFAULT_TIMEOUT = new TimeInterval(10L, TimeUnit.MINUTES);

  @Valid
  private TimeInterval timeout;
  @AdvancedConfig
  private Boolean parseOutput;
  @NotBlank
  private String dfuplusCommand;
  @NotBlank
  private String server;
  private String username;
  @InputFieldHint(style = "PASSWORD")
  private String password;

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

  /**
   * @return the parseOutput
   */
  public Boolean getParseOutput() {
    return parseOutput;
  }

  /**
   * Specify whether or not to parse the output from dfuplus.
   * <p>
   * It appears that dfuplus (on windows at least) may always exit with an exitcode of 0, which is not very helpful when trying
   * to diagnose possible errors. Set this to be true (the default) to attempt to do some parsing of the console logging from
   * dfuplus to parse errors.
   * </p>
   * 
   * @param b the parseOutput to set
   */
  public void setParseOutput(Boolean b) {
    this.parseOutput = b;
  }

  DfuplusOutputParser createParser() {
    if (getParseOutput() != null ? getParseOutput().booleanValue() : true) {
      return new SimpleOutputParser();
    }
    return new NoOpDfuplusOutputParser();
  }


  protected void execute(CommandLine cmdLine) throws ProduceException {
    int exit = 0;
    boolean success = false;
    // Create DFU command
    // String cmd = "dfuplus action=%s format=%s maxrecordsize=%d sourcefile=%s dstname=%s server=%s dstcluster=%s username=%s
    // password=%s overwrite=%d";
    DfuplusOutputParser outputParser = createParser();
    try (TeeOutputStream out = new TeeOutputStream(new Slf4jLoggingOutputStream(log, "DEBUG"), outputParser)) {
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
    success = outputParser.wasSuccessful() && exit == 0;
    if (!success) {
      throw new ProduceException("Spray exited with exit code " + exit + ", but was not successful");
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

  private File validateCmd(String cmd) throws IOException {
    File dfuPlus = new File(cmd);
    if (dfuPlus.exists() && dfuPlus.isFile() && dfuPlus.canExecute()) {
      return dfuPlus;
    }
    throw new IOException("Can't execute [" + dfuPlus.getCanonicalPath() + "]");
  }

  private class NoOpDfuplusOutputParser extends DfuplusOutputParser {

    NoOpDfuplusOutputParser() {
      super(new NullOutputStream());
    }

    @Override
    public boolean wasSuccessful() {
      return true;
    }
  }
}
