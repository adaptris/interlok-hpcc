package com.adaptris.hpcc.arguments;

import com.adaptris.core.CoreException;
import org.apache.commons.exec.CommandLine;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author mwarman
 */
public class XMLSprayFormatTest {

  @Test
  public void construct() throws Exception {
    XMLSprayFormat sprayFormat = new XMLSprayFormat();
    assertEquals("xml", sprayFormat.getFormat());
  }

  @Test
  public void prepare() throws Exception {
    XMLSprayFormat sprayFormat = new XMLSprayFormat("root");
    sprayFormat.prepare();
    sprayFormat = new XMLSprayFormat();
    try {
      sprayFormat.prepare();
      fail();
    } catch (CoreException expected){
      assertEquals("rowTag may not be null", expected.getMessage());
    }
  }

  @Test
  public void addCommandSpecificArguments() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    XMLSprayFormat sprayFormat = new XMLSprayFormat();
    sprayFormat.addArguments(cmdLine);
    assertEquals(1, cmdLine.getArguments().length);
    assertEquals("format=xml", cmdLine.getArguments()[0]);
  }

  @Test
  public void addCommandSpecificArgumentsRowTag() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    XMLSprayFormat sprayFormat = new XMLSprayFormat();
    sprayFormat.setRowTag("root");
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=xml", cmdLine.getArguments()[0]);
    assertEquals("rowtag=root", cmdLine.getArguments()[1]);
  }

  @Test
  public void addCommandSpecificArgumentsEncoding() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    XMLSprayFormat sprayFormat = new XMLSprayFormat();
    sprayFormat.setEncoding(XMLSprayFormat.ENCODING.ASCII);
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=xml", cmdLine.getArguments()[0]);
    assertEquals("encoding=ascii", cmdLine.getArguments()[1]);
  }

  @Test
  public void addCommandSpecificArgumentsMaxRecordSize() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    XMLSprayFormat sprayFormat = new XMLSprayFormat();
    sprayFormat.setMaxRecordSize(8192);
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=xml", cmdLine.getArguments()[0]);
    assertEquals("maxrecordsize=8192", cmdLine.getArguments()[1]);
  }

}