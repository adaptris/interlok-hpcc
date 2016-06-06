package com.adaptris.hpcc;

import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneRequestor;

public class DesprayFromThorTest extends ProducerCase {

  public DesprayFromThorTest(String name) {
    super(name);
  }

  @Override
  protected StandaloneRequestor retrieveObjectForSampleConfig() {
    DesprayFromThor t = new DesprayFromThor();
    t.setServer("http://192.168.56.101:8010");
    t.setDfuplusCommand("/opt/path/to/hpcc/client/tools/bin/dfuplus");
    t.setDestination(new ConfiguredProduceDestination("test::test"));
    t.setUsername("myuser");
    t.setPassword("myPassword");
    t.setDestIpAddress("192.168.56.1");
    return new StandaloneRequestor(t);
  }


}
