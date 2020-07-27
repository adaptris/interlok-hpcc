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

import static com.adaptris.core.util.DestinationHelper.logWarningIfNotNull;
import static com.adaptris.core.util.DestinationHelper.mustHaveEither;
import java.io.IOException;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.BooleanUtils;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.DestinationHelper;
import com.adaptris.core.util.LoggingHelper;
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
   * The destination represents the file will be written to.
   *
   */
  @Getter
  @Setter
  @Deprecated
  @Valid
  @Removal(version = "4.0.0", message = "Use 'logical-filename' instead")
  private ProduceDestination destination;

  /**
   * The filename to write in Thor
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  // Needs to be @NotBlank when destination is removed.
  private String logicalFilename;

  private transient boolean destWarning;

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
    logWarningIfNotNull(destWarning, () -> destWarning = true, getDestination(),
        "{} uses destination, use 'logical-filename' instead", LoggingHelper.friendlyName(this));
    mustHaveEither(getLogicalFilename(), getDestination());
    Args.notBlank(getCluster(), "cluster");
    super.prepare();
  }


  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return DestinationHelper.resolveProduceDestination(getLogicalFilename(), getDestination(), msg);
  }
}
