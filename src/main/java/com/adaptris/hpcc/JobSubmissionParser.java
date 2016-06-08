package com.adaptris.hpcc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobSubmissionParser extends DfuplusOutputParser {

  private transient Logger log = LoggerFactory.getLogger(this.getClass());

  private static final String WUID = "Submitted WUID ";
  private transient String workUnit = null;

  public JobSubmissionParser() {
    super();
  }


  @Override
  protected JobStatus getJobStatus() {
    return JobStatus.NOT_COMPLETE;
  }

  protected String getWorkUnitId() {
    return workUnit;
  }

  // D:\hpcc\5.4.2\clienttools\bin>dfuplus.exe server=http://192.168.56.102:8010 username=hpccdemo password=hpccdemo action=spray
  // dstcluster=mythor overwrite=1 srcfile=D:\hpcc\json-weatherdata\weather02\* dstname=~zzlc::json::weather
  // PREFIX=FILENAME,FILESIZE nosplit=1 nowait=1
  //
  // srcip not specified - assuming spray from local machine
  // Checking for local Dali File Server
  //
  // Fixed spraying from D:\hpcc\json-weatherdata\weather02\* on 192.168.72.83:7100 to ~zzlc::json::weather
  // Submitted WUID D20160607-142043

  @Override
  protected void processLine(String line, int logLevel) {
    if (line.startsWith(WUID)) {
      workUnit = line.substring(WUID.length()).trim();
      log.trace("WUID [{}]", workUnit);
    }
  }
}
