package com.adaptris.hpcc;

import java.io.FilterOutputStream;
import java.io.OutputStream;

public abstract class DfuplusOutputParser extends FilterOutputStream {

  public DfuplusOutputParser(OutputStream out) {
    super(out);
  }

  protected abstract boolean wasSuccessful();
}
