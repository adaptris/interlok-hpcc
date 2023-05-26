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

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.File;

import javax.validation.constraints.NotBlank;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.Getter;
import lombok.Setter;

/**
 * Spray the contents of a directory to Thor.
 *
 * <p>
 * Note that this producer <strong>ignores</strong> the current message contents and just sprays the contents of the directory specified by
 * {@link #getSourceDirectory()} using the configured dfuplus command.
 * </p>
 * <p>
 * Effectively, the program executed is going to similar to
 *
 * <pre>
 * {@code
      dfuplus action=spray srcfile=/path/to/dir/*
        dstcluster=mythor dstname=~zzlc:json:data overwrite=1 PREFIX=FILENAME,FILESIZE
        server= nosplit=1 username= password=
   }
 * </pre>
 *
 * Be aware that nosplit=1 is always added, as well as the "/*".
 * </p>
 * <p>
 * The adapter also needs a running {@code dfuplus action=dafilesrv} instance on the machine where the adapter is hosted. Thor will connect
 * to this instance for file delivery.
 * </p>
 *
 * @author lchan
 * @config spray-directory-to-thor
 *
 */
@XStreamAlias("spray-directory-to-thor")
@AdapterComponent
@ComponentProfile(summary = "Spray a directory into HPCC via dfuplus", tag = "producer,hpcc,dfuplus", recommended = {
    DfuplusConnection.class })
@DisplayOrder(order = { "logicalFilename", "cluster", "sourceDirectory", "overwrite" })
public class SprayDirectoryToThor extends SprayToThorImpl {

  /**
   * Optional prefix.
   *
   */
  @Getter
  @Setter
  private String prefix;
  /**
   * The source directory to spray into Thor.
   *
   */
  @Getter
  @Setter
  @InputFieldHint(expression = true)
  @NotBlank
  private String sourceDirectory;

  /**
   * Specify true to delete the source directory after successfully spray into HPCC.
   * <p>
   * The default is false if not explicitly specified
   * </p>
   */
  @AdvancedConfig(rare = true)
  @Getter
  @Setter
  @InputFieldDefault(value = "false")
  private Boolean deleteSourceDirectory;

  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    // Create DFU command
    // dfuplus action=spray srcfile=/var/lib/HPCCSystems/mydropzone/historical-weather/adapter-agility-historic-out/*
    // dstcluster=mythor dstname=zzlc::json::historical_weather_04 overwrite=1 PREFIX=FILENAME,FILESIZE
    // server= nosplit=1 username= password=
    try {
      CommandLine commandLine = createSprayCommand(msg);
      commandLine.addArgument(String.format("srcfile=%s", getSource(msg)));
      commandLine.addArgument(String.format("dstname=%s", endpoint));
      if (!isBlank(getPrefix())) {
        commandLine.addArgument(String.format("PREFIX=%s", getPrefix()));
      }
      commandLine.addArgument("nosplit=1");
      log.trace("Executing {}", commandLine);
      execute(commandLine);
      postSprayCleanup(msg);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  @Override
  public void prepare() throws CoreException {
    Args.notNull(getSourceDirectory(), "source-directory");
    super.prepare();
  }

  private String getSource(AdaptrisMessage msg) throws Exception {
    String result = "";
    result = String.format("%1$s%2$s*", getSourceDir(msg), File.separator);
    log.trace("Source Files [{}]", result);
    return result;
  }

  private File getSourceDir(AdaptrisMessage msg) throws Exception {
    return new File(msg.resolve(getSourceDirectory()));
  }

  private void postSprayCleanup(AdaptrisMessage msg) throws Exception {
    if (deleteSourceDirectory()) {
      File f = getSourceDir(msg);
      log.trace("Deleting [{}]", f);
      FileUtils.deleteQuietly(f);
    }
  }

  private boolean deleteSourceDirectory() {
    return BooleanUtils.toBooleanDefaultIfNull(getDeleteSourceDirectory(), false);
  }

}
