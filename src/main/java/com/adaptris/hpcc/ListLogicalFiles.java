package com.adaptris.hpcc;

import java.io.PrintWriter;

import org.apache.commons.exec.CommandLine;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageProducerImp;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.StandaloneRequestor;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Query Thor for a list of files.
 * <p>
 * This simply uses dfuplus to query Thor and get a list of logical files. The list replaces the existing message payload.
 * </p>
 * <p>
 * Note that although this is an implementation of {@link AdaptrisMessageProducerImp} the {@code AdaptrisMessageProducer#produce()}
 * methods will throw a {@link UnsupportedOperationException}. It should be used as part of a {@link StandaloneRequestor} where the
 * {@link ProduceDestination} returns the logical filename mask of the file(s) that you wish to retrieve.
 * </p>
 * 
 * @config list-logical-files-in-thor
 */
@XStreamAlias("list-logical-files-in-thor")
@AdapterComponent
@ComponentProfile(summary = "Query HPCC for a list of files", tag = "producer,hpcc,dfuplus,thor",
    recommended = {DfuplusConnection.class})
public class ListLogicalFiles extends DfuPlusWrapper {


  public ListLogicalFiles() {

  }

  @Override
  public final void produce(AdaptrisMessage msg) throws ProduceException {
    throw new UnsupportedOperationException("Use request()");
  }

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination dest) throws ProduceException {
    throw new UnsupportedOperationException("Use request()");
  }

  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg) throws ProduceException {
    return request(msg, getDestination(), monitorIntervalMs());
  }

  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg, long timeout) throws ProduceException {
    return request(msg, getDestination(), timeout);
  }

  @Override
  public final AdaptrisMessage request(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    return request(msg, destination, monitorIntervalMs());
  }


  @Override
  public AdaptrisMessage request(AdaptrisMessage msg, ProduceDestination destination, long timeoutMs) throws ProduceException {
    try {
      String dest = destination.getDestination(msg);
      CommandLine commandLine = retrieveConnection(DfuplusConnection.class).createCommand();
      commandLine.addArgument("action=list");
      commandLine.addArgument(String.format("name=%s", dest));
      log.trace("Executing {}", commandLine);
      try (PrintWriter out = new PrintWriter(msg.getWriter())) {
        ListLogicalFilesOutput parser = new ListLogicalFilesOutput(dest, out);
        executeInternal(commandLine, parser);
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }
}
