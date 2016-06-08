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
    DfuplusConnection c = new DfuplusConnection();
    c.setServer("http://192.168.56.101:8010");
    c.setDfuplusCommand("/opt/path/to/hpcc/client/tools/bin/dfuplus");
    c.setUsername("myuser");
    c.setPassword("myPassword");

    SprayDirectoryToThor p = new SprayDirectoryToThor();
    p.setCluster("mythor");
    p.setDestination(new ConfiguredProduceDestination("~test::test"));
    p.setOverwrite(true);
    p.setPrefix("FILENAME,FILESIZE");
    p.setSourceDirectoryKey("metadataKeyContainingDirectory");
    return new StandaloneProducer(c, p);
  }


}
