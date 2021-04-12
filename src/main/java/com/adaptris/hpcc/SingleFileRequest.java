package com.adaptris.hpcc;

import javax.validation.constraints.NotBlank;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.interlok.util.Args;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public abstract class SingleFileRequest extends RequestOnlyImpl {


  /**
   * The filename to write in Thor
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @NotBlank
  private String logicalFilename;

  @Override
  public void prepare() throws CoreException {
    Args.notBlank(getLogicalFilename(), "logical-filename");
    super.prepare();
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return msg.resolve(getLogicalFilename());
  }

}
