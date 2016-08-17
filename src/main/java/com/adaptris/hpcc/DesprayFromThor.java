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
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.StandaloneRequestor;
import com.adaptris.core.lms.FileBackedMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Despray a file from Thor.
 * 
 * <p>
 * Note that although this is an implementation of {@link AdaptrisMessageProducerImp} the {@code AdaptrisMessageProducer#produce()}
 * methods will throw a {@link UnsupportedOperationException}. It should be used as part of a {@link StandaloneRequestor} where the
 * {@link ProduceDestination} returns the logical filename of the file that you wish to retrieve.
 * </p>
 * <p>
 * The adapter also needs a running {@code dfuplus action=dafilesrv} instance on the machine where the adapter is hosted. Thor will
 * connect to this instance to deliver the files.
 * </p>
 * 
 * @config despray-from-thor
 */
@XStreamAlias("despray-from-thor")
@AdapterComponent
@ComponentProfile(summary = "Despray a logical file from HPCC into the current message via dfuplus",
    tag = "producer,hpcc,dfuplus,thor",
    recommended = {DfuplusConnection.class})
@DisplayOrder(order = {"destIpAddress", "tempDirectory"})
public class DesprayFromThor extends RequestOnlyImpl {

  @AdvancedConfig
  private String tempDirectory;
  private String destIpAddress;

  private transient final FileCleaningTracker tracker = new FileCleaningTracker();

  public DesprayFromThor() {

  }

  /**
   * @return the tempDir
   */
  public String getTempDirectory() {
    return tempDirectory;
  }

  /**
   * A temporary directory that we will use when despraying.
   * 
   * @param tempDir the tempDir to set (not a URL); default is null which defaults to {@code java.io.tmpdir}
   */
  public void setTempDirectory(String tempDir) {
    this.tempDirectory = tempDir;
  }


  String destIpAddress() throws IOException {
    return getDestIpAddress() != null ? getDestIpAddress() : InetAddress.getLocalHost().getHostAddress();
  }


  /**
   * @return the destIpAddress
   */
  public String getDestIpAddress() {
    return destIpAddress;
  }

  /**
   * Set the destination IP address to despray to.
   * 
   * <p>
   * If the destination IP address is not specified, then the default dfuplus action is to despray into the configured landing
   * zone (which may not be where the adapter is hosted); ultimately we want the desprayed file to be accessible to the adapter, so
   * if no ip address is configured then {@link InetAddress#getLocalHost()} is used; which may be incorrect in
   * multi-homed systems. The adapter will need a running {@code dfuplus action=dafilesrv} instance on the machine where the adapter
   * is hosted. Thor will connect to this instance to deliver the files.
   * </p>
   * 
   * @param s the destination IP Address to (the dstip argument); if not specified then
   *        {@link InetAddress#getLocalHost()}.
   */
  public void setDestIpAddress(String s) {
    this.destIpAddress = s;
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
  public AdaptrisMessage request(AdaptrisMessage msg, ProduceDestination destination, long timeoutMs) throws ProduceException {
    try {
      File destFile = createAndTrackFile(msg);
      CommandLine commandLine = retrieveConnection(DfuplusConnection.class).createCommand();
      commandLine.addArgument("action=despray");
      commandLine.addArgument(String.format("srcname=%s", destination.getDestination(msg)));
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
