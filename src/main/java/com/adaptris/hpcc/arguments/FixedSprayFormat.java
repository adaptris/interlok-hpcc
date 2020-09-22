package com.adaptris.hpcc.arguments;

import javax.validation.constraints.NotBlank;
import org.apache.commons.exec.CommandLine;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * SprayFormat implementation that allows the configuration of command specific arguments for
 * <code>format=fixed</code> sprays.
 *
 * <table>
 *   <tr><th>argument</th><th>required</th><th>description</th><th>default</th></tr>
 *   <tr><td>recordsize</td><td>yes</td><td>The fixed size of each record, in bytes.</td><td>N/A</td></tr>
 * </table>
 *
 * @author mwarman
 */
@XStreamAlias("spray-format-fixed")
public class FixedSprayFormat extends SprayFormat {

  private static final String FORMAT = "fixed";

  @NotBlank
  private Integer recordSize;

  public FixedSprayFormat(){
  }

  public FixedSprayFormat(int recordSize){
    setRecordSize(recordSize);
  }

  @Override
  public void addCommandSpecificArguments(CommandLine cmdLine) {
    cmdLine.addArgument(String.format("recordsize=%d", getRecordSize()));
  }

  @Override
  public void prepare() throws CoreException {
    try {
      Args.notNull(getRecordSize(), "recordSize");
    } catch (IllegalArgumentException e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  @Override
  public String getFormat() {
    return FORMAT;
  }

  public Integer getRecordSize() {
    return recordSize;
  }

  public void setRecordSize(Integer recordSize) {
    this.recordSize = Args.notNull(recordSize, "recordSize");;
  }
}
