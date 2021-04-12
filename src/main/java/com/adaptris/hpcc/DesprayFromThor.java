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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.IOUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageProducerImp;
import com.adaptris.core.ProduceException;
import com.adaptris.core.StandaloneRequestor;
import com.adaptris.core.lms.FileBackedMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Despray a file from Thor.
 *
 * <p>
 * Note that although this is an implementation of {@link AdaptrisMessageProducerImp} the
 * {@code AdaptrisMessageProducer#produce()} methods will throw a
 * {@link UnsupportedOperationException}. It should be used as part of a {@link StandaloneRequestor}
 * where the {@link #getLogicalFilename()} returns the logical filename of the file that you wish to
 * retrieve.
 * </p>
 * <p>
 * The adapter also needs a running {@code dfuplus action=dafilesrv} instance on the machine where
 * the adapter is hosted. Thor will connect to this instance to deliver the files.
 * </p>
 *
 * @config despray-from-thor
 */
@XStreamAlias("despray-from-thor")
@AdapterComponent
@ComponentProfile(summary = "Despray a logical file from HPCC into the current message via dfuplus",
    tag = "producer,hpcc,dfuplus,thor",
    recommended = {DfuplusConnection.class})
@NoArgsConstructor
@DisplayOrder(order = {"logicalFilename", "destIpAddress", "tempDirectory"})
public class DesprayFromThor extends SingleFileRequest {

  /**
   * A temporary directory that we will use when despraying.
   * <p>
   * default is null which defaults to {@code java.io.tmpdir}
   * </p>
   */
  @AdvancedConfig
  @Getter
  @Setter
  private String tempDirectory;
  /**
   * Set the destination IP address to despray to.
   *
   * <p>
   * If the destination IP address is not specified, then the default dfuplus action is to despray
   * into the configured landing zone (which may not be where the adapter is hosted); ultimately we
   * want the desprayed file to be accessible to the adapter, so if no ip address is configured then
   * {@link InetAddress#getLocalHost()} is used; which may be incorrect in multi-homed systems. The
   * adapter will need a running {@code dfuplus action=dafilesrv} instance on the machine where the
   * adapter is hosted. Thor will connect to this instance to deliver the files.
   * </p>
   */
  @Getter
  @Setter
  private String destIpAddress;

  private transient final FileCleaningTracker tracker = new FileCleaningTracker();


  private String destIpAddress() throws IOException {
    return getDestIpAddress() != null ? getDestIpAddress() : InetAddress.getLocalHost().getHostAddress();
  }


  private File createAndTrackFile(Object marker) throws IOException {
    File result = null;
    if (getTempDirectory() != null) {
      result = File.createTempFile(this.getClass().getSimpleName(), "", new File(getTempDirectory()));
    } else {
      result = File.createTempFile(this.getClass().getSimpleName(), "");
    }
    tracker.track(result, marker);
    return result;
  }

  private void fileToMessage(File f, AdaptrisMessage msg) throws IOException {
    // dfuplus seems to create files with a negative modtime!
    f.setLastModified(System.currentTimeMillis());
    if (msg instanceof FileBackedMessage) {
      ((FileBackedMessage) msg).initialiseFrom(f);
    } else {
      try (InputStream in = new FileInputStream(f); OutputStream out = msg.getOutputStream()) {
        IOUtils.copy(in, out);
      }
    }
  }

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, String endpoint, long timeoutMs)
      throws ProduceException {
    try {
      File destFile = createAndTrackFile(msg);
      CommandLine commandLine = retrieveConnection(DfuplusConnection.class).createCommand();
      commandLine.addArgument("action=despray");
      commandLine.addArgument(String.format("srcname=%s", endpoint));
      commandLine.addArgument(String.format("dstfile=%s", destFile.getCanonicalPath()));
      commandLine.addArgument(String.format("dstip=%s", destIpAddress()));
      commandLine.addArgument("overwrite=1");
      commandLine.addArgument("nowait=1");
      log.trace("Executing {}", commandLine);
      execute(commandLine);
      fileToMessage(destFile, msg);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }

}
