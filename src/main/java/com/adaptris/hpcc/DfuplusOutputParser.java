package com.adaptris.hpcc;

import org.apache.commons.exec.LogOutputStream;

public abstract class DfuplusOutputParser extends LogOutputStream {

  public enum JobStatus {
    SUCCESS,
    FAILURE,
    NOT_COMPLETE,
  }
  public DfuplusOutputParser() {
    super();
  }

  protected abstract JobStatus getJobStatus();

  protected abstract String getWorkUnitId();
}
