package com.adaptris.hpcc.arguments;

import com.adaptris.core.CoreException;
import com.adaptris.hpcc.arguments.FixedSprayFormat;
import org.apache.commons.exec.CommandLine;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mwarman
 */
public class FixedSprayFormatTest {

  @Test
  public void construct() throws Exception {
    FixedSprayFormat fixedSprayFormat = new FixedSprayFormat();
    assertEquals("fixed", fixedSprayFormat.getFormat());
    assertNull(fixedSprayFormat.getRecordSize());
    fixedSprayFormat = new FixedSprayFormat(125);
    assertEquals("fixed", fixedSprayFormat.getFormat());
    assertEquals(Integer.valueOf(125), fixedSprayFormat.getRecordSize());
  }

  @Test
  public void addCommandSpecificArguments() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    FixedSprayFormat fixedSprayFormat = new FixedSprayFormat(125);
    fixedSprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=fixed", cmdLine.getArguments()[0]);
    assertEquals("recordsize=125", cmdLine.getArguments()[1]);
  }

  @Test
  public void prepare() throws Exception {
    FixedSprayFormat fixedSprayFormat = new FixedSprayFormat(125);
    fixedSprayFormat.prepare();
    fixedSprayFormat = new FixedSprayFormat();
    try {
      fixedSprayFormat.prepare();
      fail();
    } catch (CoreException expected){
      assertEquals("recordSize may not be null", expected.getMessage());
    }
  }

  @Test
  public void getFormat() throws Exception {
    FixedSprayFormat fixedSprayFormat = new FixedSprayFormat();
    assertEquals("fixed", fixedSprayFormat.getFormat());
  }

  @Test
  public void getRecordSize() throws Exception {
    FixedSprayFormat fixedSprayFormat = new FixedSprayFormat();
    fixedSprayFormat.setRecordSize(125);
    assertEquals(Integer.valueOf(125), fixedSprayFormat.getRecordSize());
  }

}