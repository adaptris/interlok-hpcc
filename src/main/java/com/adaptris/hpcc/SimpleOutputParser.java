package com.adaptris.hpcc;

import static org.apache.commons.lang.StringUtils.isEmpty;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleOutputParser extends DfuplusOutputParser {

  private transient Logger log = LoggerFactory.getLogger(this.getClass());

  private static final String WUID = "Submitted WUID ";

  public SimpleOutputParser() {
    super(new ByteArrayOutputStream());
  }

  // Sample Console logging :
  // srcip not specified - assuming spray from local machine
  // Checking for local Dali File Server
  //
  // Variable spraying from C:\cygwin\tmp\adp8813954039068307266.dat on 192.168.72.83:7100 to ~zzlc::csv::farm_data_rel
  // Submitted WUID D20160421-100951
  // 0% Done
  // D20160421-100951 Finished
  // Total time taken 4 secs, Average transfer 5KB/sec
  //
  @Override
  protected boolean wasSuccessful() {
    boolean success = false;
    ByteArrayOutputStream myOut = (ByteArrayOutputStream) out;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(myOut.toByteArray())))) {
      String line = null;
      String workUnitComplete = null;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith(WUID)) {
          String workUnit = line.substring(WUID.length());
          workUnitComplete = String.format("%s Finished", workUnit);
          log.trace("WorkUnit [{}], Success Message Required = [{}]", workUnit, workUnitComplete);
          continue;
        }
        if (!isEmpty(workUnitComplete)) {
          if (line.equalsIgnoreCase(workUnitComplete)) {
            log.trace("Found [{}]", workUnitComplete);
            success = true;
            break;
          }
        }
      }
    } catch (Exception e) {
      success = false;
    }
    return success;
  }
}
