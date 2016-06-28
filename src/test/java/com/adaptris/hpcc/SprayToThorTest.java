package com.adaptris.hpcc;

import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneProducer;

public class SprayToThorTest extends ProducerCase {

  public SprayToThorTest(String name) {
    super(name);
  }

  @Override
  protected StandaloneProducer retrieveObjectForSampleConfig() {
    DfuplusConnection c = new DfuplusConnection();
    c.setServer("http://192.168.56.101:8010");
    c.setDfuplusCommand("/opt/path/to/hpcc/client/tools/bin/dfuplus");
    c.setUsername("myuser");
    c.setPassword("myPassword");

    SprayToThor p = new SprayToThor();
    p.setCluster("mythor");
    p.setDestination(new ConfiguredProduceDestination("~test::test"));
    p.setFormat(SprayToThor.FORMAT.CSV);
    p.setOverwrite(true);
    return new StandaloneProducer(c, p);
  }


}
