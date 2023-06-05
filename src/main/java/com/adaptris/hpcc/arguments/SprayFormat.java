package com.adaptris.hpcc.arguments;

import org.apache.commons.exec.CommandLine;

import com.adaptris.core.ComponentLifecycleExtension;

/**
 * @author mwarman
 */
public abstract class SprayFormat implements ComponentLifecycleExtension {

  public enum ENCODING {
    ASCII, UTF8, UTF8N, UTF16, UTF16LE, UTF16BE, UTF32, UTF32LE, UTF32BE
  }

  abstract String getFormat();

  abstract void addCommandSpecificArguments(CommandLine commandLine);

  public final void addArguments(CommandLine commandLine) {
    commandLine.addArgument(String.format("format=%s", getFormat()));
    addCommandSpecificArguments(commandLine);
  }

  final void addArgumentIfNotNull(CommandLine commandLine, String key, Object value) {
    if (value != null) {
      commandLine.addArgument(String.format("%s=%s", key, value), false);
    }
  }

}
