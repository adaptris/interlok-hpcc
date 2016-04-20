package com.adaptris.hpcc;

import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneProducer;

public class SprayDirectoryToThorTest extends ProducerCase {

  public SprayDirectoryToThorTest(String name) {
    super(name);
  }

  @Override
  protected StandaloneProducer retrieveObjectForSampleConfig() {
    SprayDirectoryToThor t = new SprayDirectoryToThor();
    t.setCluster("mythor");
    t.setServer("http://localhost:8010");
    t.setDfuplusCommand("/opt/path/to/hpcc/client/tools/bin/dfuplus");
    t.setDestination(new ConfiguredProduceDestination("~test::test"));
    t.setOverwrite(true);
    t.setUsername("myuser");
    t.setPassword("myPassword");
    t.setPrefix("FILENAME,FILESIZE");
    t.setSourceDirectoryKey("metadataKeyContainingDirectory");
    return new StandaloneProducer(t);
  }


}
