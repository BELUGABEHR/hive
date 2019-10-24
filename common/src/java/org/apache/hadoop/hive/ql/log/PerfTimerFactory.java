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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PerfTimerFactory.
 *
 * Can be used to measure and log the time spent by a piece of code.
 */
public final class PerfTimerFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(PerfTimerFactory.class);

  private static final ConcurrentMap<Class<?>, PerfTimerLogger> LOGGER_CACHE =
      new ConcurrentHashMap<>();

  private PerfTimerFactory() {
  }

  public static PerfTimer getPerfTimer(final Optional<String> sessionId,
      final Optional<HiveConf> conf, final Class<?> clazz,
      final PerfTimedAction action) {
    return getPerfTimer(sessionId, conf, clazz, action, Optional.empty());
  }

  public static PerfTimer getPerfTimer(final Optional<String> sessionId,
      final Optional<HiveConf> conf, final Class<?> clazz,
      final PerfTimedAction action, final Optional<String> extra) {
    Class<?> perfLoggerClass = MetricsPerfTimerLogger.class;
    try {
      if (conf.isPresent()) {
        perfLoggerClass = conf.get().getClassByName(
            conf.get().getVar(HiveConf.ConfVars.HIVE_PERF_LOGGER_V2));
      }
    } catch (ClassNotFoundException e) {
      LOG.error(
          "Using class {}. Configured performance logger class not found: {}",
          MetricsPerfTimerLogger.class.getName(), e.getMessage());
    }

    final boolean isCacheable =
        CachedPerfTimerLogger.class.isAssignableFrom(perfLoggerClass);

    final PerfTimerLogger newPerfLogger;
    if (isCacheable) {
      newPerfLogger = LOGGER_CACHE.computeIfAbsent(perfLoggerClass, (key) -> {
        return (PerfTimerLogger) ReflectionUtils.newInstance(key,
            conf.orElse(null));
      });
    } else {
      newPerfLogger = (PerfTimerLogger) ReflectionUtils
          .newInstance(perfLoggerClass, conf.orElse(null));
    }

    return new PerfTimer(sessionId, clazz, action, extra, newPerfLogger);
  }
}
