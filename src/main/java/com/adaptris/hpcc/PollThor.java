package com.adaptris.hpcc;

import java.util.concurrent.TimeUnit;

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
 * Poll Thor for the existence of a logical file.
 * <p>
 * The use case for this service is, if there is a long-running Job on Thor (scheduled or otherwise) that creates a logical file;
 * you need to wait for the existence of this file before carrying on with the rest of the adapter workflow (usually despraying
 * the logical file that was created).
 * </p>
 * <p>
 * Note that although this is an implementation of {@link AdaptrisMessageProducerImp} the {@code AdaptrisMessageProducer#produce()}
 * methods will throw a {@link UnsupportedOperationException}. It should be used as part of a {@link StandaloneRequestor} where the
 * {@link ProduceDestination} returns the logical filename of the file that you wish to retrieve.
 * </p>
 * 
 * @config poll-thor
 */
@XStreamAlias("poll-thor")
@AdapterComponent
@ComponentProfile(summary = "Poll HPCC for the existence of a logical file", tag = "producer,hpcc,dfuplus,thor",
    recommended = {DfuplusConnection.class})
public class PollThor extends DfuPlusWrapper {


  public PollThor() {

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
      ListOutputParser parser = new ListOutputParser(dest);
      long sleepyTime = calculateWait(0);
      while (!parser.found()) {
        executeInternal(commandLine, parser);
        if (parser.hasErrors()) {
          throw new ProduceException("Errors executing dfuplus");
        }
        if (!parser.found()) {
          log.trace("[{}] not found, retrying", dest, sleepyTime);
          TimeUnit.MILLISECONDS.sleep(sleepyTime);
          sleepyTime = calculateWait(sleepyTime);
        } else {
          break;
        }
        parser = new ListOutputParser(dest);
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }
}
