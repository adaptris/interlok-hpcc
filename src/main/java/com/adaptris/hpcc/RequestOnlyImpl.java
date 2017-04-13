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

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;

public abstract class RequestOnlyImpl extends DfuPlusWrapper {

  /**
   * UnsupportedOperationException is thrown
   * 
   */
  @Override
  public final void produce(AdaptrisMessage msg) throws ProduceException {
    throw new UnsupportedOperationException("Use request()");
  }

  /**
   * UnsupportedOperationException is thrown
   * 
   */
  @Override
  public final void produce(AdaptrisMessage msg, ProduceDestination dest) throws ProduceException {
    throw new UnsupportedOperationException("Use request()");
  }

  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg) throws ProduceException {
    return request(msg, getDestination(), monitorIntervalMs());
  }

  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg, long timeout) throws ProduceException {
    return request(msg, getDestination(), timeout);
  }

  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    return request(msg, destination, monitorIntervalMs());
  }
}
