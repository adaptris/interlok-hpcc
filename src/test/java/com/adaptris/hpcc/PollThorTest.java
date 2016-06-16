package com.adaptris.hpcc;

import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneRequestor;

public class PollThorTest extends ProducerCase {

  public PollThorTest(String name) {
    super(name);
  }

  @Override
  protected StandaloneRequestor retrieveObjectForSampleConfig() {
    DfuplusConnection c = new DfuplusConnection();
    c.setServer("http://192.168.56.101:8010");
    c.setDfuplusCommand("/opt/path/to/hpcc/client/tools/bin/dfuplus");
    c.setUsername("myuser");
    c.setPassword("myPassword");

    PollThor p = new PollThor();
    p.setDestination(new ConfiguredProduceDestination("~test::test"));
    return new StandaloneRequestor(c, p);
  }


}
