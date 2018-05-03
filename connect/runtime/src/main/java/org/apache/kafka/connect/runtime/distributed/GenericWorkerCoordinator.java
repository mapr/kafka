/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.runtime.distributed;

import java.io.Closeable;
import org.apache.kafka.common.errors.WakeupException;

public interface GenericWorkerCoordinator extends Closeable {
  public void ensureCoordinatorKnown();
  public void ensureActiveGroup();
  public void requestRejoin();
  public String memberId();
  public void wakeup();
  public void poll(long timeout) throws WakeupException;
  public String ownerUrl(String connector);
  public String ownerUrl(ConnectorTaskId task);
  public void maybeLeaveGroup();
}
