/*
 * Copyright 2016 Adaptris Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package com.adaptris.hpcc;

import static org.junit.Assert.assertEquals;
import org.apache.commons.exec.CommandLine;
import org.junit.Test;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.hpcc.arguments.CSVSprayFormat;
import com.adaptris.hpcc.arguments.FixedSprayFormat;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;

public class SprayToThorTest extends ExampleProducerCase {


  @Test
  public void testCSVFormat() throws Exception {
    SprayToThor sprayToThor = new SprayToThor();
    sprayToThor.setSprayFormat(new CSVSprayFormat());
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    sprayToThor.addFormatArguments(cmdLine);
    assertEquals(1, cmdLine.getArguments().length);
    assertEquals("format=csv", cmdLine.getArguments()[0]);
  }

  @Test
  public void testCSVFormatMaxRecordSize() throws Exception {
    SprayToThor sprayToThor = new SprayToThor();
    CSVSprayFormat sprayFormat = new CSVSprayFormat();
    sprayFormat.setMaxRecordSize(214);
    sprayToThor.setSprayFormat(sprayFormat);
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    sprayToThor.addFormatArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=csv", cmdLine.getArguments()[0]);
    assertEquals("maxrecordsize=214", cmdLine.getArguments()[1]);
  }

  @Test
  public void testFixed() throws Exception {
    SprayToThor sprayToThor = new SprayToThor();
    sprayToThor.setSprayFormat(new FixedSprayFormat(125));
    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    sprayToThor.addFormatArguments(cmdLine);
    assertEquals(2, cmdLine.getArguments().length);
    assertEquals("format=fixed", cmdLine.getArguments()[0]);
    assertEquals("recordsize=125", cmdLine.getArguments()[1]);
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
    p.setLogicalFilename("~test::test");
    p.setSprayFormat(new CSVSprayFormat());
    p.setOverwrite(true);
    return new StandaloneProducer(c, p);
  }


}
