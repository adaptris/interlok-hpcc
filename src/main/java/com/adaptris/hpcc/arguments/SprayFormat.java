package com.adaptris.hpcc.arguments;

import com.adaptris.core.ComponentLifecycleExtension;
import org.apache.commons.exec.CommandLine;

/**
 * @author mwarman
 */
public abstract class SprayFormat implements ComponentLifecycleExtension {

  abstract String getFormat();

  abstract void addCommandSpecificArguments(CommandLine commandLine);

  public final void addArguments(CommandLine commandLine){
    commandLine.addArgument(String.format("format=%s", getFormat()));
    addCommandSpecificArguments(commandLine);
  }

  final void addArgumentIfNoNull(CommandLine commandLine, String key, Object value){
    if (value != null){
      commandLine.addArgument(String.format("%s=%s", key, value), false);
    }
  }
}
