package com.adaptris.hpcc;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang.StringUtils;

import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.security.password.Password;
import com.adaptris.util.stream.Slf4jLoggingOutputStream;
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
    try (Slf4jLoggingOutputStream out = new Slf4jLoggingOutputStream(log, "DEBUG")) {
      Executor cmd = new DefaultExecutor();
      ExecuteWatchdog watchdog = new ExecuteWatchdog(timeoutMs());
      cmd.setWatchdog(watchdog);
      CommandLine commandLine = new CommandLine(getDfuplusCommand());
      commandLine.addArgument("action=spray");
      commandLine.addArgument(String.format("srcfile=%s", getSourceFiles(msg)));
      commandLine.addArgument(String.format("dstname=%s", destination.getDestination(msg)));
      commandLine.addArgument(String.format("server=%s", getServer()));
      commandLine.addArgument(String.format("dstcluster=%s", getCluster()));
      commandLine.addArgument(String.format("username=%s", getUsername()));
      commandLine.addArgument(String.format("password=%s", Password.decode(getPassword())));
      commandLine.addArgument(String.format("overwrite=%d", overwrite() ? 1 : 0));
      if (!StringUtils.isBlank(getPrefix())) {
        commandLine.addArgument(String.format("PREFIX=%d", getPrefix()));
      }
      commandLine.addArgument("nosplit=1");
      PumpStreamHandler pump = new PumpStreamHandler(out);
      cmd.setStreamHandler(pump);
      log.trace("Executing {}", commandLine);
      exit = cmd.execute(commandLine);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    if (exit != 0) {
      throw new ProduceException("Spray failed with exit code " + exit);
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

  private String getSourceFiles(AdaptrisMessage msg) {
    String dir = "";
    if (msg.headersContainsKey(getSourceDirectoryKey())) {
      dir = String.format("%1$s/*", msg.getMetadataValue(getSourceDirectoryKey()));
    }
    log.trace("Source Files [{}]", dir);
    return dir;
  }
}
