package com.adaptris.hpcc;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.lms.FileBackedMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("spray-to-thor")
public class SprayToThor extends ProduceOnlyProducerImp {

  public enum FORMAT { CSV, FIXED; }

  private String dfuplusCommand;
  private FORMAT format;
  private int maxRecordSize = 8192;
  private String destinationLogicalFileName;
  private String server;
  private String cluster;
  private String username;
  private String password;
  private boolean overwrite;

  private transient final FileCleaningTracker tracker = new FileCleaningTracker();
  
  @Override
  public void close() {
    // NOP
  }

  @Override
  public void init() throws CoreException {
    // NOP
  }

  @Override
  public void start() throws CoreException {
    // NOP
  }

  @Override
  public void stop() {
    // NOP
  }

  @Override
  public void prepare() throws CoreException {
    // NOP
  }
  
  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    File sourceFile;
    Object marker = new Object();
    
    if(msg instanceof FileBackedMessage) {
      sourceFile = ((FileBackedMessage)msg).currentSource();
    } else {
      // If the message is not file-backed, write it to a temp file
      try {
        sourceFile = File.createTempFile("adp", ".dat");
        tracker.track(sourceFile, marker);
        FileUtils.writeByteArrayToFile(sourceFile, msg.getPayload());
      } catch (IOException e) {
        throw new ProduceException("Unable to write temporary file", e);
      }
    }
    
    // Create DFU command
//    String cmd = "dfuplus action=%s format=%s maxrecordsize=%d sourcefile=%s dstname=%s server=%s dstcluster=%s username=%s password=%s overwrite=%d";
        
    try {
      String[] args = new String[] {
          dfuplusCommand,
          "action=spray",
          String.format("format=%s", getFormat().name().toLowerCase()),
          String.format("maxrecordsize=%d", getMaxRecordSize()),
          String.format("srcfile=%s", sourceFile.getCanonicalPath()),
          String.format("dstname=%s", getDestinationLogicalFileName()),
          String.format("server=%s", getServer()),
          String.format("dstcluster=%s", getCluster()),
          String.format("username=%s", getUsername()),
          String.format("password=%s", getPassword()),
          String.format("overwrite=%d", getOverwrite()?1:0)          
      };
      for(String s: args) {
        log.debug(s);
      }
      ProcessBuilder pb = new ProcessBuilder(args);
      
      Process p = pb.start();
      boolean exited = p.waitFor(10, TimeUnit.MINUTES);
      
      IOUtils.copy(p.getInputStream(), System.out);
      IOUtils.copy(p.getErrorStream(), System.err);
      
      if(!exited) {
        p.destroy();
        throw new ProduceException("Spray is taking too long.");
      } else {
        if(p.exitValue() != 0) {
          throw new ProduceException("Spray failed with exit code " + p.exitValue());
        }
      }
    } catch (IOException e) {
      throw new ProduceException("Unable to start process", e);
    } catch (InterruptedException e) {
      throw new ProduceException("We got interrupted while waiting for the process to finish.");
    }
  }

  public String getDfuplusCommand() {
    return dfuplusCommand;
  }

  public void setDfuplusCommand(String dfuplusCommand) {
    this.dfuplusCommand = dfuplusCommand;
  }

  public FORMAT getFormat() {
    return format;
  }

  public void setFormat(FORMAT format) {
    this.format = format;
  }

  public int getMaxRecordSize() {
    return maxRecordSize;
  }

  public void setMaxRecordSize(int maxRecordSize) {
    this.maxRecordSize = maxRecordSize;
  }
  
  public String getDestinationLogicalFileName() {
    return destinationLogicalFileName;
  }

  public void setDestinationLogicalFileName(String destinationLogicalFileName) {
    this.destinationLogicalFileName = destinationLogicalFileName;
  }

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public boolean getOverwrite() {
    return overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }
  
}
