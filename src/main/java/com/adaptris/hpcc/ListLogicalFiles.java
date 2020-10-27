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
import java.io.PrintWriter;
import javax.validation.Valid;
import org.apache.commons.exec.CommandLine;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.validation.constraints.ConfigDeprecated;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageProducerImp;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.StandaloneRequestor;
import com.adaptris.core.util.DestinationHelper;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Query Thor for a list of files.
 * <p>
 * This simply uses dfuplus to query Thor and get a list of logical files. The list replaces the existing message payload.
 * </p>
 * <p>
 * Note that although this is an implementation of {@link AdaptrisMessageProducerImp} the {@code AdaptrisMessageProducer#produce()}
 * methods will throw a {@link UnsupportedOperationException}. It should be used as part of a {@link StandaloneRequestor} where the
 * {@link ProduceDestination} returns the logical filename mask of the file(s) that you wish to retrieve.
 * </p>
 *
 * @config list-logical-files-in-thor
 */
@XStreamAlias("list-logical-files-in-thor")
@AdapterComponent
@ComponentProfile(summary = "Query HPCC for a list of files", tag = "producer,hpcc,dfuplus,thor",
    recommended = {DfuplusConnection.class})
@DisplayOrder(order = {"filemask"})
@NoArgsConstructor
public class ListLogicalFiles extends RequestOnlyImpl {

  /**
   * The destination represents the file will be written to.
   *
   */
  @Getter
  @Setter
  @Deprecated
  @Valid
  @ConfigDeprecated(removalVersion = "4.0.0", message = "Use 'filemask' instead", groups = Deprecated.class)
  private ProduceDestination destination;

  /**
   * The filename to write in Thor
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  // Needs to be @NotBlank when destination is removed.
  private String filemask;

  private transient boolean destWarning;

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, String filespec, long timeoutMs)
      throws ProduceException {
    try {
      CommandLine commandLine = retrieveConnection(DfuplusConnection.class).createCommand();
      commandLine.addArgument("action=list");
      commandLine.addArgument(String.format("name=%s", filespec));
      log.trace("Executing {}", commandLine);
      try (PrintWriter out = new PrintWriter(msg.getWriter())) {
        // parser is closed as part of the execution
        ListLogicalFilesOutput parser = new ListLogicalFilesOutput(filespec, out); // lgtm [java/output-resource-leak]
        executeInternal(commandLine, parser);
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }


  @Override
  public void prepare() throws CoreException {
    logWarningIfNotNull(destWarning, () -> destWarning = true, getDestination(),
        "{} uses destination, use 'filemask' instead", LoggingHelper.friendlyName(this));
    mustHaveEither(getFilemask(), getDestination());
    super.prepare();
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return DestinationHelper.resolveProduceDestination(getFilemask(), getDestination(), msg);
  }
}
