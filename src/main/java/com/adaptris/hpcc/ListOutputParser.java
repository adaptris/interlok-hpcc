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

import org.apache.oro.text.GlobCompiler;
import org.apache.oro.text.regex.Pattern;

// C:\adaptris\HPCCSystems\5.4.2\clienttools\bin>dfuplus action=list server=http://192.168.56.101:8011 name="zzlc*"
// List zzlc*
// 00000000 2016-06-16 15:55:40 18852 20308 "ERROR: Error connecting to 192.168.56.101:8011"
// 00000001 2016-06-16 15:55:40 18852 20308 "-3: connection failed Target: T>192.168.56.101, Raised in:
// ..\..\..\HPCC-Platform\system\jlib\jsocket.cpp, line 1250"
// SOAP Connection error
//
// C:\adaptris\HPCCSystems\5.4.2\clienttools\bin>dfuplus action=list server=http://192.168.56.101:8010
// name="zzlc::???::farm_data_rel"
// List zzlc::???::farm_data_rel
// zzlc::csv::farm_data_rel
//
// C:\adaptris\HPCCSystems\5.4.2\clienttools\bin>dfuplus action=list server=http://192.168.56.101:8010
// name="zzlc::???::farm_data_rel"
// List zzlc::???::farm_data_rel
// zzlc::csv::farm_data_rel
//
// C:\adaptris\HPCCSystems\5.4.2\clienttools\bin>dfuplus action=list server=http://192.168.56.101:8010 name="*zzlc*"
// List *zzlc*
// myzzlc::csv::farm_data_rel
// zzlc::csv::farm_data_rel
class ListOutputParser extends ListOutputParserImpl {

  private transient boolean ready = false;
  private transient Pattern globPattern;
  private transient boolean found = false;

  public ListOutputParser(String filespec) throws Exception {
    super(filespec);
    globPattern = new GlobCompiler().compile(filespec);
  }

  @Override
  protected void processLine(String line, int logLevel) {
    checkHasErrors(line);
    if (expectedFirstLine.equalsIgnoreCase(line)) {
      ready = true;
      return;
    }
    if (ready) {
      checkIsMatch(line);
    }
  }

  protected void checkIsMatch(String line) {
    if (matcher.matches(line, globPattern)) {
      found = true;
    }
  }

  public boolean found() {
    return found;
  }
}
