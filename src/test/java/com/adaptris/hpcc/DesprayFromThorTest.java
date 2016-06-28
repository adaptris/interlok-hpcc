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
    DfuplusConnection c = new DfuplusConnection();
    c.setServer("http://192.168.56.101:8010");
    c.setDfuplusCommand("/opt/path/to/hpcc/client/tools/bin/dfuplus");
    c.setUsername("myuser");
    c.setPassword("myPassword");

    DesprayFromThor p = new DesprayFromThor();
    p.setDestination(new ConfiguredProduceDestination("test::test"));
    p.setDestIpAddress("192.168.56.1");
    return new StandaloneRequestor(c, p);
  }


}
