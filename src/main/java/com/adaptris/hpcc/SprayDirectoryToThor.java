package com.adaptris.hpcc;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.io.File;

import org.apache.commons.exec.CommandLine;

import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Spray multiple files into Thor straight away.
 * 
 * <p>
 * Note that this producer <strong>ignores</strong> the current message contents and just sprays the contents of the directory
 * specified by {@link #getSourceDirectoryKey()} using the configured dfuplus command.
 * </p>
 * 
 * @author lchan
 *
 */
@XStreamAlias("spray-directory-to-thor")
@DisplayOrder(order = {"dfuplusCommand", "sourceDirectoryKey", "server", "cluster", "username", "password", "overwrite"})
public class SprayDirectoryToThor extends SprayToThorImpl {

  private String prefix;
  private String sourceDirectoryKey;

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    int exit = 0;
    // Create DFU command
    // dfuplus action=spray srcfile=/var/lib/HPCCSystems/mydropzone/historical-weather/adapter-agility-historic-out/*
    // dstcluster=mythor dstname=zzlc::json::historical_weather_04 overwrite=1 PREFIX=FILENAME,FILESIZE
    // server= nosplit=1 username= password=
    try {
      CommandLine commandLine = createCommand();
      commandLine.addArgument(String.format("srcfile=%s", getSource(msg)));
      commandLine.addArgument(String.format("dstname=%s", destination.getDestination(msg)));
      if (!isBlank(getPrefix())) {
        commandLine.addArgument(String.format("PREFIX=%s", getPrefix()));
      }
      commandLine.addArgument("nosplit=1");
      execute(commandLine);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  /**
   * @return the prefix
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * @param s the prefix to set
   */
  public void setPrefix(String s) {
    this.prefix = s;
  }

  /**
   * @return the sourceDirectoryKey
   */
  public String getSourceDirectoryKey() {
    return sourceDirectoryKey;
  }

  /**
   * Set the metadata containing the source directory to upload.
   * 
   * @param s the sourceDirectoryKey to set
   */
  public void setSourceDirectoryKey(String s) {
    this.sourceDirectoryKey = s;
  }

  private String getSource(AdaptrisMessage msg) throws Exception {
    String result = "";
    if (msg.headersContainsKey(getSourceDirectoryKey())) {
      String dir = msg.getMetadataValue(getSourceDirectoryKey());
      // Now turn it into a canonical path so that it's platform correct as
      // it appears to D:/hpcc/json-weatherdata/weather02/* doesn't work well.
      result = new File(String.format("%1$s/*", dir)).getCanonicalPath();
    }
    log.trace("Source Files [{}]", result);
    return result;
  }
}
