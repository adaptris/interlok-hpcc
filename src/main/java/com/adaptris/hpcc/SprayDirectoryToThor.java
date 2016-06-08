package com.adaptris.hpcc;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.io.File;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FileUtils;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Spray the contents of a directory to Thor.
 * 
 * <p>
 * Note that this producer <strong>ignores</strong> the current message contents and just sprays the contents of the directory
 * specified by {@link #getSourceDirectoryKey()} using the configured dfuplus command.
 * </p>
 * <p>
 * Effectively, the program executed is going to similar to
 * <pre>
 * {@code 
      dfuplus action=spray srcfile=/path/to/dir/*
        dstcluster=mythor dstname=~zzlc:json:data overwrite=1 PREFIX=FILENAME,FILESIZE
        server= nosplit=1 username= password=
   }
 * </pre>
 * Be aware that nosplit=1 is always added, as well as the "/*".
 * </p>
 * <p>
 * The adapter also needs a running {@code dfuplus action=dafilesrv} instance which can be connected to from
 * Thor running on the machine where the adapter is hosted.
 * </p>
 * 
 * @author lchan
 * @config spray-directory-to-thor
 *
 */
@XStreamAlias("spray-directory-to-thor")
@DisplayOrder(order = {"dfuplusCommand", "server", "cluster", "username", "password", "sourceDirectoryKey", "overwrite"})
public class SprayDirectoryToThor extends SprayToThorImpl {

  private String prefix;
  private String sourceDirectoryKey;
  @AdvancedConfig
  private Boolean deleteSourceDirectory;

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
      postSprayCleanup(msg);
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
      // Now turn it into a canonical path so that it's platform correct as
      // it appears to D:/hpcc/json-weatherdata/weather02/* doesn't work well.
      String dir = getSourceDir(msg).getCanonicalPath();
      result = String.format("%1$s%2$s*", dir, File.separator);
    }
    log.trace("Source Files [{}]", result);
    return result;
  }

  private File getSourceDir(AdaptrisMessage msg) throws Exception {
    File f = null;
    if (msg.headersContainsKey(getSourceDirectoryKey())) {
      f = new File(msg.getMetadataValue(getSourceDirectoryKey()));
    }
    return f;
  }

  private void postSprayCleanup(AdaptrisMessage msg) throws Exception {
    if (deleteSourceDirectory()) {
      File f = getSourceDir(msg);
      log.trace("Deleting [{}]", f);
      FileUtils.deleteQuietly(f);
    }
  }

  /**
   * @return the deleteSourceDirectory
   */
  public Boolean getDeleteSourceDirectory() {
    return deleteSourceDirectory;
  }

  /**
   * Whether or not to delete the source directory after uploading (if there are no errors).
   * 
   * @param b true to delete the source directory after processing; default if not configured is false.
   */
  public void setDeleteSourceDirectory(Boolean b) {
    this.deleteSourceDirectory = b;
  }

  private boolean deleteSourceDirectory() {
    return getDeleteSourceDirectory() != null ? getDeleteSourceDirectory().booleanValue() : false;
  }
}
