package com.adaptris.hpcc;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.io.File;
import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.NoOpConnection;
import com.adaptris.core.SharedConnection;
import com.adaptris.core.util.Args;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Wrapper around the key dfuplus parameters as an {@link AdaptrisConnection} implementation.
 * <p>
 * While there is no explicit connection required for dfuplus (as it's a commandline executable); it is still desirable to
 * have the key fields (namely server/username/password) wrapped as a connection so that it is configurable as a
 * {@link SharedConnection} which means less boilerplate configuration in the future.
 * </p>
 * 
 * 
 * @author lchan
 * @config dfuplus-connection
 */
@XStreamAlias("dfuplus-connection")
@AdapterComponent
@ComponentProfile(summary = "Wrapper around the dfuplus executable", tag = "connections,hpcc,dfuplus")
@DisplayOrder(order = {"dfuplusCommand", "server", "username", "password"})
public class DfuplusConnection extends NoOpConnection {

  @NotBlank
  private String dfuplusCommand;
  @NotBlank
  private String server;
  private String username;
  @InputFieldHint(style = "PASSWORD")
  private String password;


  public DfuplusConnection() {
    super();
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

  public CommandLine createCommand() throws PasswordException, IOException {
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


}
