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

  protected CommandLine createSprayCommand() throws PasswordException, IOException {
    CommandLine cmdLine = retrieveConnection(DfuplusConnection.class).createCommand();
    cmdLine.addArgument("action=spray");
    cmdLine.addArgument(String.format("dstcluster=%s", getCluster()));
    cmdLine.addArgument(String.format("overwrite=%d", overwrite() ? 1 : 0));
    cmdLine.addArgument("nowait=1");
    return cmdLine;
  }

}
