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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JobSubmissionParser extends DfuplusOutputParser {

  private transient Logger log = LoggerFactory.getLogger("com.adaptris.hpcc.OutputParser");

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
    log.trace("Processing Line [{}]", line);
    if (line.startsWith(WUID)) {
      workUnit = line.substring(WUID.length()).trim();
      log.trace("WUID [{}]", workUnit);
    }
  }
}
