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

package org.apache.hadoop.hive.ql.lockmgr.zookeeper;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task which deletes ZNodes for Hive locking which have not been used in a long
 * time.
 */
public class ZNodeCleaner extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(ZNodeCleaner.class);
  private static final long MAX_BACKOFF = TimeUnit.MINUTES.toMillis(60L);
  private final CuratorFramework curatorFramework;
  private final String nameSpace;

  public ZNodeCleaner(final CuratorFramework curatorFramework,
      final String nameSpace) {
    super("ZNodeCleaner");
    this.curatorFramework = curatorFramework;
    this.nameSpace = nameSpace;

    super.setDaemon(true);
    super.setPriority(Thread.MIN_PRIORITY);
  }

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(TimeUnit.DAYS.toMillis(1L));

        // There may be other instances of this cleaner running in the cluster.
        // They should not all be running at the same time, so add some random
        Thread.sleep(ThreadLocalRandom.current().nextLong(MAX_BACKOFF));

        LOG.info("Running ZNode cleaner");
        cleanNodes(ZKPaths.makePath(this.nameSpace, ""));
      } catch (InterruptedException e) {
        return;
      } catch (Exception e) {
        LOG.warn("Failed to clean all nodes. Will try again later.", e);
      }
    }
  }

  private void cleanNodes(final String currentPath) throws Exception {
    final List<String> childNodeNames =
        this.curatorFramework.getChildren().forPath(currentPath);

    for (final String childNodeName : childNodeNames) {
      cleanNodes(ZKPaths.makePath(currentPath, childNodeName));
    }

    final Stat stat = this.curatorFramework.checkExists().forPath(currentPath);
    if (stat != null && stat.getNumChildren() == 0) {
      final long age = System.currentTimeMillis() - stat.getMtime();
      if (age >= TimeUnit.DAYS.toMillis(14L)) {
        this.curatorFramework.delete().forPath(currentPath);
      }
    }
  }
}
