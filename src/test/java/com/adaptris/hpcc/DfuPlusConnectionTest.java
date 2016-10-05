package com.adaptris.hpcc;

import static org.junit.Assert.assertEquals;

import org.apache.commons.exec.CommandLine;
import org.junit.Test;

public class DfuPlusConnectionTest {

  @Test
  public void testSourceIp() throws Exception {
    
    DfuplusConnection conn = new DfuplusConnection();
    conn.setDfuplusCommand("/bin/dfuplus");
    conn.setSourceIp("1.2.3.4");
    conn.setServer("2.3.4.5");
    
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    CommandLine cmd = conn.addArguments(cmdLine);
    assertEquals("server=2.3.4.5", cmd.getArguments()[0]);
    assertEquals("srcip=1.2.3.4", cmd.getArguments()[1]);
  }

}
