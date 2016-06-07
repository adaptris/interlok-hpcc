package com.adaptris.hpcc;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.StreamPumper;

import com.adaptris.core.util.ManagedThreadFactory;

class ManagedPumpStreamHandler extends PumpStreamHandler {

  private static final ManagedThreadFactory MTF = new ManagedThreadFactory();

  public ManagedPumpStreamHandler() {
      this(System.out, System.err);
  }

  public ManagedPumpStreamHandler(final OutputStream outAndErr) {
      this(outAndErr, outAndErr);
  }

  public ManagedPumpStreamHandler(final OutputStream out, final OutputStream err) {
      this(out, err, null);
  }

  public ManagedPumpStreamHandler(final OutputStream out, final OutputStream err, final InputStream input) {
    super(out, err, input);
  }

  protected Thread createPump(final InputStream is, final OutputStream os, final boolean closeWhenExhausted) {
    String name = Thread.currentThread().getName();
    final Thread result = MTF.newThread(new StreamPumper(is, os, closeWhenExhausted));
    result.setName(name);
    result.setDaemon(true);
    return result;
  }
}
