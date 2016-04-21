package com.adaptris.hpcc;

import java.io.File;
import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.FileUtils;

import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.lms.FileBackedMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("spray-to-thor")
@DisplayOrder(order = {"dfuplusCommand", "format", "maxRecordSize", "server", "cluster", "username", "password", "overwrite"})
public class SprayToThor extends SprayToThorImpl {

  public enum FORMAT { CSV, FIXED; }

  private FORMAT format;
  private int maxRecordSize = 8192;

  private transient final FileCleaningTracker tracker = new FileCleaningTracker();
  
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
    
    try {
      CommandLine commandLine = createCommand();
      commandLine.addArgument(String.format("format=%s", getFormat().name().toLowerCase()));
      commandLine.addArgument(String.format("maxrecordsize=%d", getMaxRecordSize()));
      commandLine.addArgument(String.format("srcfile=%s", sourceFile.getCanonicalPath()));
      commandLine.addArgument(String.format("dstname=%s", destination.getDestination(msg)));
      execute(commandLine);
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
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
}
