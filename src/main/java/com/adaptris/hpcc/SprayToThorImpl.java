package com.adaptris.hpcc;

import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.security.exc.PasswordException;

public abstract class SprayToThorImpl extends DfuPlusWrapper {

  @NotBlank
  private String cluster;
  private Boolean overwrite;


  /**
   * @see com.adaptris.core.AdaptrisMessageProducerImp #produce(AdaptrisMessage, ProduceDestination)
   */
  @Override
  public void produce(AdaptrisMessage msg) throws ProduceException {
    produce(msg, getDestination());
  }

  /**
   * UnsupportedOperationException is thrown
   * 
   * @see com.adaptris.core.AdaptrisMessageProducerImp#request(AdaptrisMessage)
   */
  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg) throws ProduceException {
    throw new UnsupportedOperationException("Request Reply is not supported");
  }

  /**
   * UnsupportedOperationException is thrown
   * 
   * @see com.adaptris.core.AdaptrisMessageProducerImp#request(AdaptrisMessage, long)
   */
  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg, long timeout) throws ProduceException {
    throw new UnsupportedOperationException("Request Reply is not supported");
  }

  /**
   * UnsupportedOperationException is thrown
   * 
   * @see com.adaptris.core.AdaptrisMessageProducerImp
   *      #request(AdaptrisMessage,ProduceDestination)
   */
  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    throw new UnsupportedOperationException("Request Reply is not supported");
  }

  /**
   * UnsupportedOperationException is thrown
   * 
   * @see com.adaptris.core.AdaptrisMessageProducerImp #request(AdaptrisMessage,
   *      ProduceDestination, long)
   */
  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg, ProduceDestination destination, long timeout) throws ProduceException {
    throw new UnsupportedOperationException("Request Reply is not supported");
  }


  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
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

  protected CommandLine createCommand() throws PasswordException, IOException {
    CommandLine cmdLine = super.createCommand();
    cmdLine.addArgument("action=spray");
    cmdLine.addArgument(String.format("dstcluster=%s", getCluster()));
    cmdLine.addArgument(String.format("overwrite=%d", overwrite() ? 1 : 0));
    return cmdLine;
  }

}
