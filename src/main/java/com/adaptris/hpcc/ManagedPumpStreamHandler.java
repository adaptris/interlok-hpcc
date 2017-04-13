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

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.StreamPumper;

import com.adaptris.core.util.ManagedThreadFactory;

class ManagedPumpStreamHandler extends PumpStreamHandler {

  private static final ManagedThreadFactory MTF = new ManagedThreadFactory();

  public ManagedPumpStreamHandler(final OutputStream outAndErr) {
    super(outAndErr, outAndErr);
  }

  protected Thread createPump(final InputStream is, final OutputStream os, final boolean closeWhenExhausted) {
    String name = Thread.currentThread().getName();
    final Thread result = MTF.newThread(new StreamPumper(is, os, closeWhenExhausted));
    result.setName(name);
    result.setDaemon(true);
    return result;
  }
}
