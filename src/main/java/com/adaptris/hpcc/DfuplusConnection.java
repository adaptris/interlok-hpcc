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

import java.io.File;
import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.NoOpConnection;
import com.adaptris.core.SharedConnection;
import com.adaptris.core.util.Args;
import com.adaptris.interlok.resolver.ExternalResolver;
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
  @InputFieldHint(style = "PASSWORD", external = true)
  private String password;

  @AdvancedConfig
  private String sourceIp;
  @AdvancedConfig
  private Integer transferBufferSize;
  @AdvancedConfig
  private Integer throttle;
  @AdvancedConfig
  private Boolean replicate;
  @AdvancedConfig
  private Boolean noRecover;

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
    return addArguments(cmdLine);
  }
  
  public CommandLine addArguments(CommandLine cmdLine) throws PasswordException {
    cmdLine.addArgument(String.format("server=%s", getServer()));
    if (!isBlank(getUsername())) {
      cmdLine.addArgument(String.format("username=%s", getUsername()));
    }
    if (!isBlank(getPassword())) {
      cmdLine.addArgument(String.format("password=%s", Password.decode(ExternalResolver.resolve(getPassword()))));
    }
    if (getSourceIp() != null) {
      cmdLine.addArgument(String.format("srcip=%s", getSourceIp()));
    }
    if (getReplicate() != null) {
      cmdLine.addArgument(String.format("replicate=%d", getReplicate().booleanValue() ? 1 : 0));
    }
    if (getNoRecover() != null) {
      cmdLine.addArgument(String.format("norecover=%d", getNoRecover().booleanValue() ? 1 : 0));
    }
    if (getThrottle() != null) {
      cmdLine.addArgument(String.format("throttle=%d", getThrottle().intValue()));

    }
    if (getTransferBufferSize() != null) {
      cmdLine.addArgument(String.format("transferbuffersize=%d", getTransferBufferSize().intValue()));
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

  /**
   * Return the source IP address to use.
   */
  public String getSourceIp() {
    return sourceIp;
  }

  /**
   * Set the IP address for the local machine (useful on multi-homed machines where the automatic detection guesses wrong) which is
   * equivalent to using {@code srcip=} parameter.
   * 
   * @param sourceIp the source IP.
   */
  public void setSourceIp(String sourceIp) {
    this.sourceIp = sourceIp;
  }

  /**
   * @return the transferBufferSize
   */
  public Integer getTransferBufferSize() {
    return transferBufferSize;
  }

  /**
   * Maps to the {@code transferbuffersize} argument.
   * 
   * @param i the transferBufferSize to set; if not specified, will not be passed as an argument.
   */
  public void setTransferBufferSize(Integer i) {
    this.transferBufferSize = i;
  }

  /**
   * @return the throttle
   */
  public Integer getThrottle() {
    return throttle;
  }

  /**
   * Maps to the {@code throttle} argument.
   * 
   * @param i the throttle to set; if not specified, will not be passed as an argument.
   */
  public void setThrottle(Integer i) {
    this.throttle = i;
  }

  /**
   * 
   * @return the replicate
   */
  public Boolean getReplicate() {
    return replicate;
  }

  /**
   * Maps to the {@code replicate} argument.
   * 
   * @param b true/false, if not specified, will not be passed as an argument.
   */
  public void setReplicate(Boolean b) {
    this.replicate = b;
  }

  /**
   * @return the norecover flag.
   */
  public Boolean getNoRecover() {
    return noRecover;
  }

  /**
   * Maps to the {@code norecover} argument.
   * 
   * @param b true/false, if not specified, will not be passed as an argument.
   */
  public void setNoRecover(Boolean b) {
    this.noRecover = b;
  }


}
