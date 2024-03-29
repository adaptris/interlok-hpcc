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

import com.adaptris.core.StandaloneRequestor;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;

public class DesprayFromThorTest extends ExampleProducerCase {

  @Override
  protected StandaloneRequestor retrieveObjectForSampleConfig() {
    DfuplusConnection c = new DfuplusConnection();
    c.setServer("http://192.168.56.101:8010");
    c.setDfuplusCommand("/opt/path/to/hpcc/client/tools/bin/dfuplus");
    c.setUsername("myuser");
    c.setPassword("myPassword");

    DesprayFromThor p = new DesprayFromThor();
    p.setLogicalFilename("test::test");
    p.setDestIpAddress("192.168.56.1");
    return new StandaloneRequestor(c, p);
  }

}
