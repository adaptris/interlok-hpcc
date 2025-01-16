package com.adaptris.hpcc.arguments;

import jakarta.validation.constraints.NotBlank;
import org.apache.commons.exec.CommandLine;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * SprayFormat implementation that allows the configuration of command specific arguments for
 * <code>format=xml</code> sprays.
 *
 * <table>
 *   <tr><td>rowTag</td><td>yes</td></td><td>The XML tag identifying each record.</td><td>N/A</td></tr>
 *   <tr><td>encoding</td><td>no</td><td>One of the following: utf8 utf8n utf16 utf16le utf16be utf32 utf32le utf32be</td><td>utf8</td></tr>
 *   <tr><td>maxrecordsize</td><td>no</td><td>The maximum size of each record, in bytes.</td><td>8192</td></tr>
 * </table>
 *
 * <p>NOTE: Defaults are driven by dfuplus command them selves, they will not be set unless explicitly set.</p>
 *
 * @author mwarman
 */
@XStreamAlias("spray-format-xml")
public class XMLSprayFormat extends SprayFormat {

  private static final String FORMAT = "xml";

  @NotBlank
  private String rowTag;
  @AdvancedConfig
  private ENCODING encoding;
  @AdvancedConfig
  private Integer maxRecordSize;

  public XMLSprayFormat(){
  }

  public XMLSprayFormat(String rowTag){
    setRowTag(rowTag);
  }

  @Override
  public void prepare() throws CoreException {
    try {
      Args.notNull(getRowTag(), "rowTag");
    } catch (IllegalArgumentException e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  @Override
  public void addCommandSpecificArguments(CommandLine cmdLine) {
    addArgumentIfNotNull(cmdLine, "rowtag", getRowTag());
    addArgumentIfNotNull(cmdLine, "encoding", encoding());
    addArgumentIfNotNull(cmdLine, "maxrecordsize", getMaxRecordSize());
  }

  public String getRowTag() {
    return rowTag;
  }

  public void setRowTag(String rowTag) {
    this.rowTag = Args.notNull(rowTag, "rowTag");
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

}
