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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PerfLogger implementation that logs information to SLF4J facilities.
 */
public class LoggingPerfTimerLogger implements CachedPerfTimerLogger {

  protected final Logger log = LoggerFactory.getLogger(LoggingPerfTimerLogger.class);
  private long startTime;
  private long stopTime;

  @Override
  public void start(final Optional<String> sessionId, final Class<?> clazz, final String action,
      final Optional<String> extra, final long startTime) {
    this.startTime = startTime;
    if (log.isDebugEnabled()) {
      log.debug("<PERFLOG session=[{}] class=[{}] method=[{}] extra=[{}] start=[{}]>",
          sessionId.orElse(""), clazz.getName(), action, extra.orElse(""),
          TimeUnit.NANOSECONDS.toMillis(this.startTime));
    }
  }

  @Override
  public void stop(final Class<?> clazz, final String action,
      final Optional<String> extra, final long stopTime) {
    this.stopTime = stopTime;
    if (log.isDebugEnabled()) {
      log.debug(
          "</PERFLOG class=[{}] method=[{}] from=[{}] extra=[{}] start=[{}] "
              + "end=[{}] duration=[{}ms]>",
          clazz.getName(), action, extra.orElse(""),
          TimeUnit.NANOSECONDS.toMillis(this.startTime),
          TimeUnit.NANOSECONDS.toMillis(this.stopTime),
          TimeUnit.NANOSECONDS.toMillis(this.stopTime - this.startTime));
    }
  }

  public long getStartTime() {
    return startTime;
  }

  public long getStopTime() {
    return stopTime;
  }

  @Override
  public String toString() {
    return "LoggingPerfLogger [startTime=" + startTime + ", stopTime=" + stopTime
        + "]";
  }

}
