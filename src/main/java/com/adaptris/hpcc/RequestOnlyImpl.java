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
