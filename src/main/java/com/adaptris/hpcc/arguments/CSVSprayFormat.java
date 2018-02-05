package com.adaptris.hpcc.arguments;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.commons.exec.CommandLine;

/**
 * SprayFormat implementation that allows the configuration of command specific arguments for
 * <code>format=csv</code> sprays.
 *
 * <table>
 *   <tr><td>encoding</td><td>no</td><td>One of the following: ascii, utf8, utf8n, utf16, utf16le, utf16be, utf32, utf32le, utf32be</td><td>ascii</td></tr>
 *   <tr><td>maxrecordsize</td><td>no</td><td>The maximum size of each record, in bytes.</td><td>8192</td></tr>
 *   <tr><td>separator</td><td>no</td><td>The field delimiter.</td><td> comma (\,)</td></tr>
 *   <tr><td>terminator</td><td>no</td><td>The record delimiter.</td><td>line feed or carriage return linefeed (\r,\r\n)</td></tr>
 *   <tr><td>quote</td><td>no</td><td>The string quote character.</td><td> single quote (’).</td></tr>
 * </table>
 *
 * <p>NOTE: Defaults are driven by dfuplus command them selves, they will not be set unless explicitly set.</p>
 *
 * @author mwarman
 */
@XStreamAlias("spray-format-csv")
public class CSVSprayFormat extends SprayFormat {

  private static final String FORMAT = "csv";

  @AdvancedConfig
  private ENCODING encoding;
  @AdvancedConfig
  private Integer maxRecordSize;
  @AdvancedConfig
  private String separator;
  @AdvancedConfig
  private String terminator;
  @AdvancedConfig
  private String quote;

  @Override
  public void prepare() throws CoreException {

  }

  @Override
  public void addCommandSpecificArguments(CommandLine cmdLine) {
    addArgumentIfNotNull(cmdLine, "encoding", encoding());
    addArgumentIfNotNull(cmdLine, "maxrecordsize", getMaxRecordSize());
    addArgumentIfNotNull(cmdLine, "separator", getSeparator());
    addArgumentIfNotNull(cmdLine, "terminator", getTerminator());
    addArgumentIfNotNull(cmdLine, "quote", getQuote());
  }

  @Override
  public String getFormat() {
    return FORMAT;
  }

  public void setEncoding(ENCODING encoding) {
    this.encoding = Args.notNull(encoding, "encoding");
  }

  public ENCODING getEncoding() {
    return encoding;
  }

  public String encoding() {
    return encoding != null ? getEncoding().name().toLowerCase() : null;
  }

  public void setMaxRecordSize(Integer maxRecordSize) {
    this.maxRecordSize = Args.notNull(maxRecordSize, "maxRecordSize");
  }

  public Integer getMaxRecordSize() {
    return maxRecordSize;
  }

  public void setSeparator(String separator) {
    this.separator = Args.notEmpty(separator, "separator");
  }

  public String getSeparator() {
    return separator;
  }

  public void setQuote(String quote) {
    this.quote = Args.notEmpty(quote, "quote");
  }

  public String getQuote() {
    return quote;
  }

  public void setTerminator(String terminator) {
    this.terminator = Args.notEmpty(terminator, "terminator");
  }

  public String getTerminator() {
    return terminator;
  }
}
