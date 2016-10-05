package com.adaptris.hpcc;

import org.apache.commons.exec.LogOutputStream;

abstract class DfuplusOutputParser extends LogOutputStream {

  enum JobStatus {
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
