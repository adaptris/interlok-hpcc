package com.adaptris.hpcc.arguments;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.commons.exec.CommandLine;
import org.junit.jupiter.api.Test;

/**
 * @author mwarman
 */
public class JSONSprayFormatTest {

  @Test
  public void construct() throws Exception {
    JSONSprayFormat sprayFormat = new JSONSprayFormat();
    assertEquals("json", sprayFormat.getFormat());
  }

  @Test
  public void prepare() throws Exception {
    JSONSprayFormat sprayFormat = spy(new JSONSprayFormat());
    sprayFormat.prepare();
    verify(sprayFormat).prepare();
  }

  @Test
  public void addCommandSpecificArguments() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    JSONSprayFormat sprayFormat = new JSONSprayFormat();
    sprayFormat.addArguments(cmdLine);
    assertEquals(1, cmdLine.getArguments().length);
    assertEquals("format=json", cmdLine.getArguments()[0]);
  }

  @Test
  public void addCommandSpecificArgumentsRowPath() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    JSONSprayFormat sprayFormat = new JSONSprayFormat();
    sprayFormat.setRowPath("/");
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=json", cmdLine.getArguments()[0]);
    assertEquals("rowpath=/", cmdLine.getArguments()[1]);
  }

  @Test
  public void addCommandSpecificArgumentsMaxRecordSize() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    JSONSprayFormat sprayFormat = new JSONSprayFormat();
    sprayFormat.setMaxRecordSize(8192);
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=json", cmdLine.getArguments()[0]);
    assertEquals("maxrecordsize=8192", cmdLine.getArguments()[1]);
  }

}