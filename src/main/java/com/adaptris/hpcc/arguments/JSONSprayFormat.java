package com.adaptris.hpcc.arguments;

import javax.validation.constraints.NotBlank;
import org.apache.commons.exec.CommandLine;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * SprayFormat implementation that allows the configuration of command specific arguments for
 * <code>format=json</code> sprays.
 *
 * <table>
 *   <tr><td>rowpath</td><td>no</td></td><td></td><td>"/"</td></tr>
 *   <tr><td>maxrecordsize</td><td>no</td><td>The maximum size of each record, in bytes.</td><td>8192</td></tr>
 * </table>
 *
 * <p>NOTE: Defaults are driven by dfuplus command them selves, they will not be set unless explicitly set.</p>
 *
 * @author mwarman
 */
@XStreamAlias("spray-format-json")
public class JSONSprayFormat extends SprayFormat {

  private static final String FORMAT = "json";

  @NotBlank
  private String rowPath;
  @AdvancedConfig
  private Integer maxRecordSize;

  @Override
  public void prepare() throws CoreException {
  }

  @Override
  public void addCommandSpecificArguments(CommandLine cmdLine) {
    addArgumentIfNotNull(cmdLine, "rowpath", getRowPath());
    addArgumentIfNotNull(cmdLine, "maxrecordsize", getMaxRecordSize());
  }

  public String getRowPath() {
    return rowPath;
  }

  public void setRowPath(String rowPath) {
    this.rowPath = Args.notNull(rowPath, "rowPath");
  }

  @Override
  public String getFormat() {
    return FORMAT;
  }

  public void setMaxRecordSize(Integer maxRecordSize) {
    this.maxRecordSize = Args.notNull(maxRecordSize, "maxRecordSize");
  }

  public Integer getMaxRecordSize() {
    return maxRecordSize;
  }

}
