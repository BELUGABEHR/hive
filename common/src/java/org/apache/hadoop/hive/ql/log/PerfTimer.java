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

import java.time.Duration;
import java.util.Optional;

import com.google.common.base.Preconditions;

/**
 * A tool to provide timing of time-critical sections of the code. This class
 * extends {@link AutoCloseable} and should be used in conjunction with
 * try-with-resources with block.
 *
 * <pre>
 * try (PerfTimer timer = PerfTimerFactory.getTimer()) {
 * ...
 * }
 * </pre>
 */
public class PerfTimer implements AutoCloseable {

  private final Optional<String> sessionId;
  private final Class<?> clazz;
  private final PerfTimedAction action;
  private final PerfTimerLogger log;
  private final Optional<String> extra;
  private final long startTime;
  private long stopTime = -1L;
  private boolean isClosed = false;

  public PerfTimer(final Optional<String> sessionId, final Class<?> clazz,
      final PerfTimedAction action, final Optional<String> extra,
      final PerfTimerLogger perfLogger) {
    this.sessionId = sessionId;
    this.clazz = clazz;
    this.action = action;
    this.log = perfLogger;
    this.extra = extra;
    this.startTime = System.nanoTime();
    this.log.start(sessionId, clazz, action.getName(), extra, this.startTime);
  }

  public Optional<String> getSessionId() {
    return sessionId;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getStopTime() {
    return stopTime;
  }

  public Duration getDuration() {
    Preconditions.checkState(isClosed);
    return Duration.ofNanos(this.stopTime - this.startTime);
  }

  @Override
  public void close() {
    if (!isClosed) {
      this.stopTime = System.nanoTime();
      this.isClosed = true;
      log.stop(this.clazz, this.action.getName(), extra, this.stopTime);
    }
  }

  @Override
  public String toString() {
    return "PerfTimer [sessionId=" + sessionId + ", clazz=" + clazz
        + ", action=" + action + ", extra=" + extra + ", startTime=" + startTime
        + ", stopTime=" + stopTime + ", isClosed=" + isClosed + "]";
  }

}
