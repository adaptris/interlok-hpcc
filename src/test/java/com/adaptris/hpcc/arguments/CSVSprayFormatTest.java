package com.adaptris.hpcc.arguments;

import org.apache.commons.exec.CommandLine;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author mwarman
 */
public class CSVSprayFormatTest {

  @Test
  public void construct() throws Exception {
    CSVSprayFormat csvSprayFormat = new CSVSprayFormat();
    assertEquals("csv", csvSprayFormat.getFormat());
  }

  @Test
  public void prepare() throws Exception {
    CSVSprayFormat csvSprayFormat = spy(new CSVSprayFormat());
    csvSprayFormat.prepare();
    verify(csvSprayFormat).prepare();
  }

  @Test
  public void addCommandSpecificArguments() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    CSVSprayFormat sprayFormat = new CSVSprayFormat();
    sprayFormat.addArguments(cmdLine);
    assertEquals(1, cmdLine.getArguments().length);
    assertEquals("format=csv", cmdLine.getArguments()[0]);
  }

  @Test
  public void addCommandSpecificArgumentsEncoding() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    CSVSprayFormat sprayFormat = new CSVSprayFormat();
    sprayFormat.setEncoding(CSVSprayFormat.ENCODING.ASCII);
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=csv", cmdLine.getArguments()[0]);
    assertEquals("encoding=ascii", cmdLine.getArguments()[1]);
  }

  @Test
  public void addCommandSpecificArgumentsMaxRecordSize() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    CSVSprayFormat sprayFormat = new CSVSprayFormat();
    sprayFormat.setMaxRecordSize(8192);
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=csv", cmdLine.getArguments()[0]);
    assertEquals("maxrecordsize=8192", cmdLine.getArguments()[1]);
  }

  @Test
  public void addCommandSpecificArgumentSeparator() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    CSVSprayFormat sprayFormat = new CSVSprayFormat();
    sprayFormat.setSeparator("\\,");
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=csv", cmdLine.getArguments()[0]);
    assertEquals("separator=\\,", cmdLine.getArguments()[1]);
  }

  @Test
  public void addCommandSpecificArgumentTerminator() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    CSVSprayFormat sprayFormat = new CSVSprayFormat();
    sprayFormat.setTerminator("\\r,\\r\\n");
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=csv", cmdLine.getArguments()[0]);
    assertEquals("terminator=\\r,\\r\\n", cmdLine.getArguments()[1]);
  }

  @Test
  public void addCommandSpecificArgumentQuote() throws Exception {
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    CSVSprayFormat sprayFormat = new CSVSprayFormat();
    sprayFormat.setQuote("\'");
    sprayFormat.addArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=csv", cmdLine.getArguments()[0]);
    assertEquals("quote='", cmdLine.getArguments()[1]);
  }

}