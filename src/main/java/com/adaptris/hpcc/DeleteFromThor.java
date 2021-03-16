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

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.LogOutputStream;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageProducerImp;
import com.adaptris.core.ProduceException;
import com.adaptris.core.StandaloneRequestor;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * Delete a logical file from Thor.
 *
 * <p>
 * Note that although this is an implementation of {@link AdaptrisMessageProducerImp} the
 * {@code AdaptrisMessageProducer#produce()} methods will throw a
 * {@link UnsupportedOperationException}. It should be used as part of a {@link StandaloneRequestor}
 * where the {@link #getLogicalFilename()} returns the logical filename mask of the file(s) that you
 * wish to retrieve.
 * </p>
 *
 * @config delete-from-thor
 */
@XStreamAlias("delete-from-thor")
@AdapterComponent
@ComponentProfile(summary = "Delete a specific file from HPCC", tag = "producer,hpcc,dfuplus,thor",
    recommended = {DfuplusConnection.class})
@DisplayOrder(order = {"logicalFilename"})
@NoArgsConstructor
public class DeleteFromThor extends SingleFileRequest {


  @Override
  public AdaptrisMessage doRequest(AdaptrisMessage msg, String endpoint,
      long timeoutMs) throws ProduceException {
    try {
      CommandLine commandLine = retrieveConnection(DfuplusConnection.class).createCommand();
      commandLine.addArgument("action=remove");
      commandLine.addArgument(String.format("name=%s", endpoint));
      log.trace("Executing {}", commandLine);
      DeleteOutputParser p = new DeleteOutputParser();
      executeInternal(commandLine, p);
      log.info("Delete on [{}]: {}", endpoint, p.actualState);
    }
    catch (AbortJobException e) {
      throw ExceptionHelper.wrapProduceException(generateExceptionMessage(e), e);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }


  private static class DeleteOutputParser extends LogOutputStream {
    enum DeleteState {
      Unknown("Unknown"),
      Removing("Removing"),
      Deleted("Deleted File"),
      NotFound("File not found");

      String jobText;

      DeleteState(String txt) {
        jobText = txt;
      }

    }

    private DeleteState actualState = DeleteState.Unknown;

    @Override
    protected void processLine(String line, int logLevel) {
      for (DeleteState state : DeleteState.values()) {
        if (line.startsWith(state.jobText)) {
          actualState = state;
          break;
        }
      }
    }
  }

}
