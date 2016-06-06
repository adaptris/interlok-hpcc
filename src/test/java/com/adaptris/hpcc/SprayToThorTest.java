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
    SprayToThor t = new SprayToThor();
    t.setCluster("mythor");
    t.setServer("http://192.168.56.101:8010");
    t.setDfuplusCommand("/opt/path/to/hpcc/client/tools/bin/dfuplus");
    t.setDestination(new ConfiguredProduceDestination("~test::test"));
    t.setFormat(SprayToThor.FORMAT.CSV);
    t.setOverwrite(true);
    t.setUsername("myuser");
    t.setPassword("myPassword");
    return new StandaloneProducer(t);
  }


}
