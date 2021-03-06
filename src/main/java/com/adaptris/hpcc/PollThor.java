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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.exec.CommandLine;
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
 * Poll Thor for the existence of a logical file.
 * <p>
 * The use case for this service is, if there is a long-running Job on Thor (scheduled or otherwise)
 * that creates a logical file; you need to wait for the existence of this file before carrying on
 * with the rest of the adapter workflow (usually despraying the logical file that was created).
 * </p>
 * <p>
 * Note that although this is an implementation of {@link AdaptrisMessageProducerImp} the
 * {@code AdaptrisMessageProducer#produce()} methods will throw a
 * {@link UnsupportedOperationException}. It should be used as part of a {@link StandaloneRequestor}
 * where the {@link #getLogicalFilename()} returns the logical filename of the file that you wish to
 * retrieve.
 * </p>
 *
 * @config poll-thor
 */
@XStreamAlias("poll-thor")
@AdapterComponent
@ComponentProfile(summary = "Poll HPCC for the existence of a logical file", tag = "producer,hpcc,dfuplus,thor",
    recommended = {DfuplusConnection.class})
@DisplayOrder(order = {"logicalFilename"})
@NoArgsConstructor
public class PollThor extends SingleFileRequest {

  private transient Future<Void> fileCheckStatus = null;


  @Override
  public void stop() {
    if (fileCheckStatus != null) {
      fileCheckStatus.cancel(true);
    }
    fileCheckStatus = null;
  }

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, String endpoint, long timeoutMs)
      throws ProduceException {
    try {
      CommandLine commandLine = retrieveConnection(DfuplusConnection.class).createCommand();
      commandLine.addArgument("action=list");
      commandLine.addArgument(String.format("name=%s", endpoint));
      log.trace("Executing {}", commandLine);
      ListOutputParser parser = new ListOutputParser(endpoint); // lgtm [java/output-resource-leak]
      fileCheckStatus = executor.submit(new WaitForFile(parser, commandLine, endpoint));
      fileCheckStatus.get(maxWaitMs(), TimeUnit.MILLISECONDS);
    }
    catch (AbortJobException | InterruptedException | TimeoutException e) {
      throw ExceptionHelper.wrapProduceException(generateExceptionMessage(e), e);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    } finally {
      fileCheckStatus = null;
    }
    return msg;
  }

  private class WaitForFile implements Callable<Void> {

    private String filespec;
    private ListOutputParser outputParser;
    private String threadName;
    private CommandLine cmd;

    WaitForFile(ListOutputParser initialStatus, CommandLine commandline, String filespec) {
      this.filespec = filespec;
      outputParser = initialStatus;
      cmd = commandline;
      threadName = Thread.currentThread().getName();
    }

    @Override
    public Void call() throws Exception {
      Thread.currentThread().setName(threadName);
      long sleepyTime = calculateWait(0);
      while (!outputParser.found()) {
        executeInternal(cmd, outputParser);
        if (outputParser.hasErrors()) {
          throw new ProduceException("Errors executing dfuplus");
        }
        if (!outputParser.found()) {
          log.trace("[{}] not found, retrying", filespec);
          TimeUnit.MILLISECONDS.sleep(sleepyTime);
          sleepyTime = calculateWait(sleepyTime);
        } else {
          break;
        }
        outputParser = new ListOutputParser(filespec);
      }
      return null;
    }

  }
}
