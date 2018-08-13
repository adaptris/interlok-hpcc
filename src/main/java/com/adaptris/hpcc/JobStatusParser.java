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

import static org.apache.commons.lang.StringUtils.isBlank;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JobStatusParser extends DfuplusOutputParser {


  private enum JobState {
    unknown(
        JobStatus.FAILURE),
    scheduled(
        JobStatus.NOT_COMPLETE),
    queued(
        JobStatus.NOT_COMPLETE),
    started(
        JobStatus.NOT_COMPLETE),
    aborted(
        JobStatus.FAILURE),
    failed(
        JobStatus.FAILURE),
    finished(
        JobStatus.SUCCESS),
    monitoring(
        JobStatus.SUCCESS),
    aborting(
        JobStatus.FAILURE),
    deleted(
        JobStatus.FAILURE);

    JobStatus mappedStatus;

    JobState(JobStatus s) {
      mappedStatus = s;
    }
  }


  private transient Logger log = LoggerFactory.getLogger("com.adaptris.hpcc.OutputParser");
  private transient String workUnit = null;
  private transient JobStatus jobStatus = null;
  private transient JobState jobState = null;

  private transient Map<String, JobState> workUnitMap = null;

  public JobStatusParser(String wuid) throws AbortJobException {
    super();
    if (isBlank(wuid)) {
      throw new AbortJobException("WUID is null");
    }
    workUnit = wuid;
    jobStatus = JobStatus.NOT_COMPLETE;
    jobState = JobState.unknown;
    workUnitMap = initMappings();
  }

  private Map<String, JobState> initMappings() {
    Map<String, JobState> map = new HashMap<>();
    for (JobState s : JobState.values()) {
      if (s == JobState.finished) {
        map.put(String.format("%s Finished", workUnit), s);
      } else {
        map.put(String.format("%s status: %s", workUnit, s.name()), s);
      }
    }
    return Collections.unmodifiableMap(map);
  }

  @Override
  protected JobStatus getJobStatus() {
    return jobStatus;
  }

  protected String getWorkUnitId() {
    return workUnit;
  }

  // Sample Console logging :
  // D:\hpcc\5.4.2\clienttools\bin>dfuplus.exe action=status server=http://192.168.56.102:8010 wuid=D20160607-142043
  // 1% Done, 6m 38s left (2/94MB @230KB/sec) current rate=230KB/sec [0/1nodes]

  // D:\hpcc\5.4.2\clienttools\bin>dfuplus.exe action=status server=http://192.168.56.102:8010 wuid=D20160607-132253
  // D20160607-132253 Finished
  // Total time taken 5m 14s, Average transfer 154596KB/sec

  // D:\hpcc\5.4.2\clienttools\bin>dfuplus.exe action=status server=http://192.168.56.102:8010 wuid=D20160607-132253
  // D20160607-132253 status: failed - Total time taken 5m 14s, Average transfer 154596KB/sec @Override
  protected void processLine(String line, int logLevel) {
    log.trace("Processing Line [{}]", line);
    JobStatus oldStatus = jobStatus;
    for (String key : workUnitMap.keySet()) {
      if (line.startsWith(key)) {
        jobState = workUnitMap.get(key);
        jobStatus = jobState.mappedStatus;
        break;
      }
    }
    if (oldStatus != jobStatus) {
      log.trace("Status change for WUID [{}], now [{}({})]", workUnit, jobStatus.name(), jobState.name());
    }
  }
}
