/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.metrics.common;

import java.lang.reflect.Constructor;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Class that manages a static Metric instance for this process.
 */
public class MetricsFactory {

  private static AtomicReference<Metrics> metrics = new AtomicReference<>();

  /**
   * Initializes static Metrics instance.
   */
  public synchronized static void init(HiveConf conf) throws Exception {
    if (metrics.get() == null) {
      Class<?> metricsClass = conf
          .getClassByName(conf.getVar(HiveConf.ConfVars.HIVE_METRICS_CLASS));
      Constructor<?> constructor = metricsClass.getConstructor(HiveConf.class);
      metrics.set((Metrics) constructor.newInstance(conf));
    }
  }

  /**
   * Returns static Metrics instance, null if not initialized or closed.
   */
  public static Optional<Metrics> getInstance() {
    return Optional.ofNullable(metrics.get());
  }

  /**
   * Closes and removes static Metrics instance.
   */
  public synchronized static void close() throws Exception {
    if (metrics.get() != null) {
      metrics.getAndSet(null).close();
    }
  }
}
