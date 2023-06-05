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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.commons.exec.CommandLine;
import org.junit.jupiter.api.Test;

public class DfuPlusConnectionTest {

  @Test
  public void testSourceIp() throws Exception {

    DfuplusConnection conn = new DfuplusConnection();
    conn.setDfuplusCommand("/bin/dfuplus");
    conn.setSourceIp("1.2.3.4");
    conn.setServer("2.3.4.5");

    CommandLine cmdLine = new CommandLine("/bin/dfuplus");
    CommandLine cmd = conn.addArguments(cmdLine);
    assertEquals("server=2.3.4.5", cmd.getArguments()[0]);
    assertEquals("srcip=1.2.3.4", cmd.getArguments()[1]);
  }

}
