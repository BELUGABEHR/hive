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

package org.apache.hadoop.hive.ql.log;

import java.util.Optional;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsScope;

/**
 * A PerfLogger that logs to SLF4J and provides data to the {@link Metrics}
 * sub-system.
 */
public class MetricsPerfTimerLogger extends LoggingPerfTimerLogger {

  private Optional<Metrics> metrics = Optional.empty();
  private MetricsScope scope = null;

  @Override
  public void start(final Optional<String> sessionId, final Class<?> clazz,
      final String action, final Optional<String> extra, final long startTime) {
    super.start(sessionId, clazz, action, extra, startTime);
    this.metrics = Optional.ofNullable(MetricsFactory.getInstance());
    if (this.metrics.isPresent()) {
      this.scope =
          this.metrics.get().createScope(MetricsConstant.API_PREFIX + action);
    }
  }

  @Override
  public void stop(final Class<?> clazz, final String action,
      final Optional<String> extra, final long stopTime) {
    if (metrics.isPresent()) {
      this.metrics.get().endScope(this.scope);
    }
    super.stop(clazz, action, extra, stopTime);
  }

  @Override
  public String toString() {
    return "MetricsPerfLogger [metrics=" + metrics + ", scope=" + scope
        + ", toString()=" + super.toString() + "]";
  }
}
