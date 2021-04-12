/*
 * Copyright 2016 Adaptris Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package com.adaptris.hpcc;

import java.io.IOException;
import javax.validation.constraints.NotBlank;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.BooleanUtils;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.Args;
import com.adaptris.security.exc.PasswordException;
import lombok.Getter;
import lombok.Setter;

public abstract class SprayToThorImpl extends DfuPlusWrapper {

  @NotBlank
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  private String cluster;

  @Getter
  @Setter
  @InputFieldDefault(value = "false")
  private Boolean overwrite;


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
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, String endpoint, long timeout)
      throws ProduceException {
    throw new UnsupportedOperationException("Request Reply is not supported");
  }

  protected boolean overwrite() {
    return BooleanUtils.toBooleanDefaultIfNull(getOverwrite(), false);
  }

  protected CommandLine createSprayCommand(AdaptrisMessage msg) throws PasswordException, IOException {
    CommandLine cmdLine = retrieveConnection(DfuplusConnection.class).createCommand();
    cmdLine.addArgument("action=spray");
    cmdLine.addArgument(String.format("dstcluster=%s", msg.resolve(getCluster())));
    cmdLine.addArgument(String.format("overwrite=%d", overwrite() ? 1 : 0));
    cmdLine.addArgument("nowait=1");
    return cmdLine;
  }


  @Override
  public void prepare() throws CoreException {
    Args.notBlank(getLogicalFilename(), "logical-filename");
    Args.notBlank(getCluster(), "cluster");
    super.prepare();
  }


  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return msg.resolve(getLogicalFilename());
  }
}
