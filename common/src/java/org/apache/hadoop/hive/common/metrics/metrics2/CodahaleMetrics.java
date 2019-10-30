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

package org.apache.hadoop.hive.common.metrics.metrics2;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsScope;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Codahale-backed Metrics implementation.
 */
public class CodahaleMetrics implements org.apache.hadoop.hive.common.metrics.common.Metrics {

  public static final Logger LOGGER = LoggerFactory.getLogger(CodahaleMetrics.class);

  public final MetricRegistry metricRegistry = new MetricRegistry();

  private ConcurrentMap<String, Timer> timers;
  private ConcurrentMap<String, Counter> counters;
  private ConcurrentMap<String, Meter> meters;
  private ConcurrentMap<String, Gauge<?>> gauges;

  private HiveConf conf;
  private final Set<Closeable> reporters = new HashSet<>();

  private final ThreadLocal<HashMap<String, CodahaleMetricsScope>> threadLocalScopes =
      new ThreadLocal<HashMap<String, CodahaleMetricsScope>>() {
        @Override
        protected HashMap<String, CodahaleMetricsScope> initialValue() {
          return new HashMap<>();
        }
      };

  public class CodahaleMetricsScope implements MetricsScope {

    private final String name;
    private final Timer timer;
    private Timer.Context timerContext;

    private boolean isOpen = false;

    /**
     * Instantiates a named scope - intended to only be called by Metrics, so locally scoped.
     * @param name - name of the variable
     */
    private CodahaleMetricsScope(String name) {
      this.name = name;
      this.timer = CodahaleMetrics.this.getTimer(name);
      open();
    }

    /**
     * Opens scope, and makes note of the time started, increments run counter
     *
     */
    public void open() {
      if (!isOpen) {
        isOpen = true;
        this.timerContext = timer.time();
        CodahaleMetrics.this.incrementCounter(MetricsConstant.ACTIVE_CALLS + name);
      } else {
        LOGGER.warn("Scope named " + name + " is not closed, cannot be opened.");
      }
    }

    /**
     * Closes scope, and records the time taken
     */
    public void close() {
      if (isOpen) {
        timerContext.close();
        CodahaleMetrics.this.decrementCounter(MetricsConstant.ACTIVE_CALLS + name);
      } else {
        LOGGER.warn("Scope named " + name + " is not open, cannot be closed.");
      }
      isOpen = false;
    }
  }

  public CodahaleMetrics(HiveConf conf) {
    this.conf = conf;

    timers = new ConcurrentHashMap<>();
    counters = new ConcurrentHashMap<>();
    meters = new ConcurrentHashMap<>();
    gauges = new ConcurrentHashMap<>();

    //register JVM metrics
    registerAll("gc", new GarbageCollectorMetricSet());
    registerAll("buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
    registerAll("memory", new MemoryUsageGaugeSet());
    registerAll("threads", new ThreadStatesGaugeSet());
    registerAll("classLoading", new ClassLoadingGaugeSet());

    //initialize reporters
    initReporting();
  }


  @Override
  public void close() throws Exception {
    for (Closeable reporter : reporters) {
      try {
        reporter.close();
      } catch (Exception e) {
        LOGGER.warn("Error while closing reporter {}", reporter, e);
      }
    }
    for (Map.Entry<String, Metric> metric : metricRegistry.getMetrics().entrySet()) {
      metricRegistry.remove(metric.getKey());
    }
    timers.clear();
    counters.clear();
    meters.clear();
  }

  @Override
  public void startStoredScope(String name) {
    if (threadLocalScopes.get().containsKey(name)) {
      threadLocalScopes.get().get(name).open();
    } else {
      threadLocalScopes.get().put(name, new CodahaleMetricsScope(name));
    }
  }

  @Override
  public void endStoredScope(String name) {
    if (threadLocalScopes.get().containsKey(name)) {
      threadLocalScopes.get().get(name).close();
      threadLocalScopes.get().remove(name);
    }
  }

  public MetricsScope getStoredScope(String name) throws IllegalArgumentException {
    if (threadLocalScopes.get().containsKey(name)) {
      return threadLocalScopes.get().get(name);
    } else {
      throw new IllegalArgumentException("No metrics scope named " + name);
    }
  }

  public MetricsScope createScope(String name) {
    return new CodahaleMetricsScope(name);
  }

  public void endScope(MetricsScope scope) {
    ((CodahaleMetricsScope) scope).close();
  }

  @Override
  public void incrementCounter(String name) {
    incrementCounter(name, 1L);
  }

  @Override
  public void incrementCounter(final String name, final long increment) {
    counters.computeIfAbsent(name, key -> {
      Counter counter = new Counter();
      metricRegistry.register(key, counter);
      return counter;
    }).inc(increment);
  }

  @Override
  public void decrementCounter(String name) {
    decrementCounter(name, 1L);
  }

  @Override
  public void decrementCounter(final String name, final long decrement) {
    final Counter counter = counters.get(name);
    if (counter != null) {
      counter.dec(decrement);
    }
  }

  @Override
  public void addGauge(String name, final MetricsVariable<?> variable) {
    Gauge<?> gauge = new Gauge<Object>() {
      @Override
      public Object getValue() {
        return variable.getValue();
      }
    };
    addGaugeInternal(name, gauge);
  }

  @Override
  public void removeGauge(final String name) {
    gauges.computeIfPresent(name, (key, value) -> {
      final boolean removed = metricRegistry.remove(name);
      LOGGER.debug("Gauge [{}] removed from registry: {}", key, removed);
      return null;
    });
  }

  @Override
  public void addRatio(String name, MetricsVariable<Integer> numerator,
      MetricsVariable<Integer> denominator) {
    Preconditions.checkArgument(numerator != null, "Numerator must not be null");
    Preconditions.checkArgument(denominator != null, "Denominator must not be null");

    MetricVariableRatioGauge gauge = new MetricVariableRatioGauge(numerator, denominator);
    addGaugeInternal(name, gauge);
  }

  private void addGaugeInternal(final String name, final Gauge<?> gauge) {
    gauges.compute(name, (key, value) -> {
      final boolean removed = metricRegistry.remove(name);
      if (removed) {
        LOGGER.warn("A Gauge with name [{}] already exists. "
            + " The old gauge will be overwritten, but this is not recommended", key);
      }
      metricRegistry.register(name, gauge);
      return value;
    });
  }

  @Override
  public void markMeter(final String name) {
    meters.computeIfAbsent(name, key -> {
      Meter meter = new Meter();
      metricRegistry.register(key, meter);
      return meter;
    }).mark();
  }

  private Timer getTimer(final String name) {
    return timers.computeIfAbsent(name, key -> {
      Timer timer = new Timer(new ExponentiallyDecayingReservoir());
      metricRegistry.register(key, timer);
      return timer;
    });
  }

  private void registerAll(String prefix, MetricSet metricSet) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(prefix + "." + entry.getKey(), (MetricSet) entry.getValue());
      } else {
        metricRegistry.register(prefix + "." + entry.getKey(), entry.getValue());
      }
    }
  }

  @VisibleForTesting
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @VisibleForTesting
  public String dumpJson() throws Exception {
    ObjectMapper jsonMapper = new ObjectMapper().registerModule(
      new MetricsModule(TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, false));
    return jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metricRegistry);
  }

  /**
   * Initializes reporters from HIVE_CODAHALE_METRICS_REPORTER_CLASSES or
   * HIVE_METRICS_REPORTER if the former is not defined. Note: if both confs are
   * defined, only HIVE_CODAHALE_METRICS_REPORTER_CLASSES will be used.
   */
  private void initReporting() {
    try {
      initCodahaleMetricsReporterClasses();
    } catch (Exception e1) {
      LOGGER.warn("Could not initiate Codahale Metrics Reporter Classes", e1);
      try {
        initMetricsReporter();
      } catch (Exception e2) {
        LOGGER.warn("Unable to initialize metrics reporting", e2);
      }
    }
    if (reporters.isEmpty()) {
      // log a warning in case no reporters were successfully added
      LOGGER.warn("No reporters configured for codahale metrics!");
    }
  }

  /**
   * Initializes reporting using HIVE_CODAHALE_METRICS_REPORTER_CLASSES.
   * @return whether initialization was successful or not
   */
  private void initCodahaleMetricsReporterClasses() {
    final String reporterClassesConf =
        conf.getVar(HiveConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES);
    final Iterable<String> reporterClasses = Splitter.on(",").trimResults()
        .omitEmptyStrings().split(reporterClassesConf);

    for (String reporterClass : reporterClasses) {
      Class<?> name = null;
      try {
        name = conf.getClassByName(reporterClass);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(
            "Unable to instantiate metrics reporter class " + reporterClass
                + " from conf HIVE_CODAHALE_METRICS_REPORTER_CLASSES",
            e);
      }
      try {
        // Note: Hadoop metric reporter does not support tags. We create a
        // single reporter for all metrics.
        Constructor<?> constructor =
            name.getConstructor(MetricRegistry.class, HiveConf.class);
        CodahaleReporter reporter =
            (CodahaleReporter) constructor.newInstance(metricRegistry, conf);
        reporter.start();
        reporters.add(reporter);
      } catch (NoSuchMethodException | InstantiationException
          | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalArgumentException(
            "Unable to instantiate using constructor(MetricRegistry, HiveConf)"
                + " for reporter " + reporterClass
                + " from conf HIVE_CODAHALE_METRICS_REPORTER_CLASSES",
            e);
      }
    }
  }

  /**
   * Initializes reporting using HIVE_METRICS+REPORTER.
   * @return whether initialization was successful or not
   */
  private void initMetricsReporter() {

    Iterable<String> metricsReporterNames =
        Splitter.on(",").trimResults().omitEmptyStrings()
            .split(conf.getVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER));

    for (String metricsReportingName : metricsReporterNames) {
      MetricsReporting reporter = null;
      try {
        reporter =
            MetricsReporting.valueOf(metricsReportingName.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new RuntimeException(
            "Invalid reporter name " + metricsReportingName, e);
      }
      CodahaleReporter codahaleReporter = null;
      switch (reporter) {
      case CONSOLE:
        codahaleReporter = new ConsoleMetricsReporter(metricRegistry, conf);
        break;
      case JMX:
        codahaleReporter = new JmxMetricsReporter(metricRegistry, conf);
        break;
      case JSON_FILE:
        codahaleReporter = new JsonFileMetricsReporter(metricRegistry, conf);
        break;
      case HADOOP2:
        codahaleReporter = new Metrics2Reporter(metricRegistry, conf);
        break;
      default:
        LOGGER.warn("Unhandled reporter " + reporter + " provided.");
        continue;
      }
      codahaleReporter.start();
      reporters.add(codahaleReporter);
    }
  }
}
