package com.adaptris.hpcc;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.StreamPumper;

import com.adaptris.core.util.ManagedThreadFactory;

class ManagedPumpStreamHandler extends PumpStreamHandler {

  private static final ManagedThreadFactory MTF = new ManagedThreadFactory();

  public ManagedPumpStreamHandler(final OutputStream outAndErr) {
    super(outAndErr, outAndErr);
  }

  protected Thread createPump(final InputStream is, final OutputStream os, final boolean closeWhenExhausted) {
    String name = Thread.currentThread().getName();
    final Thread result = MTF.newThread(new StreamPumper(is, os, closeWhenExhausted));
    result.setName(name);
    result.setDaemon(true);
    return result;
  }
}
