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
import java.io.IOException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.FileUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.validation.constraints.ConfigDeprecated;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.lms.FileBackedMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.hpcc.arguments.SprayFormat;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Spray the contents of the current message into Thor.
 *
 * <p>
 * The adapter also needs a running {@code dfuplus action=dafilesrv} instance on the machine where the adapter is hosted. Thor will
 * connect to this instance for file delivery.
 * </p>
 *
 * @author lchan
 * @config spray-to-thor
 *
 */
@XStreamAlias("spray-to-thor")
@AdapterComponent
@ComponentProfile(summary = "Spray the current message into HPCC via dfuplus", tag = "producer,hpcc,dfuplus",
    recommended = {DfuplusConnection.class})
@DisplayOrder(order = {"logicalFilename", "cluster", "sprayFormat", "overwrite", "tempDirectory"})
public class SprayToThor extends SprayToThorImpl {

  @Deprecated
  public enum FORMAT { CSV, FIXED; }

  private SprayFormat sprayFormat;

  @AdvancedConfig
  private String tempDirectory;

  @Deprecated
  @ConfigDeprecated(removalVersion = "4.0.0", message = "use 'spray-format' instead", groups = Deprecated.class)
  private FORMAT format;
  @Deprecated
  @ConfigDeprecated(removalVersion = "4.0.0", message = "use 'spray-format' instead", groups = Deprecated.class)
  private Integer maxRecordSize;

  private transient final FileCleaningTracker tracker = new FileCleaningTracker();

  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint)
      throws ProduceException {
    File sourceFile = saveFile(msg, msg);
    try {
      CommandLine commandLine = createSprayCommand(msg);
      addFormatArguments(commandLine);
      commandLine.addArgument(String.format("srcfile=%s", sourceFile.getCanonicalPath()));
      commandLine.addArgument(String.format("dstname=%s", endpoint));
      log.trace("Executing {}", commandLine);
      execute(commandLine);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  void addFormatArguments(CommandLine commandLine){
    if(getSprayFormat() == null) {
      log.warn("Use spray-format instead");
      commandLine.addArgument(String.format("format=%s", getFormat().name().toLowerCase()));
      commandLine.addArgument(String.format("maxrecordsize=%d", maxRecordSize()));
    } else {
      getSprayFormat().addArguments(commandLine);
    }
  }

  /**
   * @deprecated since 3.7 use {@link #getSprayFormat()} instead.
   */
  @Deprecated
  @ConfigDeprecated(removalVersion = "4.0.0", message = "use 'spray-format' instead", groups = Deprecated.class)
  public FORMAT getFormat() {
    return format;
  }

  /**
   * @deprecated since 3.7 use {@link #setSprayFormat(SprayFormat)} instead.
   */
  @Deprecated
  @Removal(version = "4.0.0", message = "use 'spray-format' instead")
  public void setFormat(FORMAT format) {
    this.format = format;
  }

  /**
   * @deprecated since 3.7 use {@link #getSprayFormat()} instead.
   */
  @Deprecated
  @ConfigDeprecated(removalVersion = "4.0.0", message = "use 'spray-format' instead", groups = Deprecated.class)
  public Integer getMaxRecordSize() {
    return maxRecordSize;
  }

  /**
   * @deprecated since 3.7 use {@link #setSprayFormat(SprayFormat)} instead.
   */
  @Deprecated
  @Removal(version = "4.0.0", message = "use 'spray-format' instead")
  public void setMaxRecordSize(Integer maxRecordSize) {
    this.maxRecordSize = maxRecordSize;
  }

  @Deprecated
  private int maxRecordSize() {
    return getMaxRecordSize() != null ? getMaxRecordSize() : 8192;
  }

  public SprayFormat getSprayFormat() {
    return sprayFormat;
  }

  public void setSprayFormat(SprayFormat sprayFormat) {
    this.sprayFormat = sprayFormat;
  }

  /**
   * @return the tempDir
   */
  public String getTempDirectory() {
    return tempDirectory;
  }

  /**
   * If specified then messages that are not {@link FileBackedMessage} will be stored in this location prior to spray.
   *
   * @param tempDir the tempDir to set; default is null which defaults to {@code java.io.tmpdir}
   */
  public void setTempDirectory(String tempDir) {
    tempDirectory = tempDir;
  }

  private File saveFile(AdaptrisMessage msg, Object marker) throws ProduceException {
    File result = null;
    if (msg instanceof FileBackedMessage) {
      result = ((FileBackedMessage) msg).currentSource();
    } else {
      // If the message is not file-backed, write it to a temp file
      try {
        if (getTempDirectory() != null) {
          result = File.createTempFile("adp", ".dat", new File(getTempDirectory()));
        } else {
          result = File.createTempFile("adp", ".dat");
        }
        tracker.track(result, marker);
        FileUtils.writeByteArrayToFile(result, msg.getPayload());
      } catch (IOException e) {
        throw new ProduceException("Unable to write temporary file", e);
      }
    }
    return result;
  }

}
