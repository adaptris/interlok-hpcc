package com.adaptris.hpcc;

import java.util.concurrent.TimeUnit;

import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.util.TimeInterval;

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

  long timeoutMs() {
    return getTimeout() != null ? getTimeout().toMilliseconds() : DEFAULT_TIMEOUT.toMilliseconds();
  }

}
