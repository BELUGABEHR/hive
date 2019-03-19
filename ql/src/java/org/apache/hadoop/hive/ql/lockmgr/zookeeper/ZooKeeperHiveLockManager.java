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

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.utils.ZKPaths.PathAndNode;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver.LockedDriverState;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManagerCtx;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A {@link HiveLockManager} that stores the locks in ZooKeeper.
 */
public class ZooKeeperHiveLockManager implements HiveLockManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(ZooKeeperHiveLockManager.class);

  static final private LogHelper console = new LogHelper(LOG);

  private CuratorFramework curatorFramework;

  HiveLockManagerCtx ctx;

  /** All ZK locks are created under this root ZNode. */
  private String zkNamespace;

  private long sleepTime;
  private int numRetriesForLock;

  private final String clientIP;

  /**
   * Constructor.
   */
  public ZooKeeperHiveLockManager() {
    String cip = "UNKNOWN";
    try {
      cip = InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      LOG.warn("Could not determine local host IP address", e);
    }
    clientIP = cip;
  }

  /**
   * @param ctx The lock manager context (containing the Hive configuration
   *          file) Start the ZooKeeper client based on the zookeeper cluster
   *          specified in the conf.
   **/
  @Override
  public void setContext(HiveLockManagerCtx ctx) throws LockException {
    final HiveConf conf = ctx.getConf();

    this.ctx = ctx;

    sleepTime =
        conf.getTimeVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES,
            TimeUnit.MILLISECONDS);
    numRetriesForLock = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES);

    zkNamespace = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_NAMESPACE);
    final String rootNodePath = ZKPaths.makePath(zkNamespace, "");

    try {
      curatorFramework = CuratorFrameworkSingleton.getInstance(conf);

      // Create the root node if it does not exist. This could simply create the
      // node and fail, but even if the creation fails, the creation actions is
      // logged in the ZK logs, so it is better to check before creating it.
      if (curatorFramework.checkExists().forPath(rootNodePath) == null) {
        curatorFramework.create().withMode(CreateMode.PERSISTENT)
            .forPath(rootNodePath);
      }
    } catch (KeeperException ke) {
      if (ke.code() == Code.NODEEXISTS) {
        LOG.debug("Root node {} already exists.", rootNodePath);
      } else {
        throw new LockException(
            ErrorMsg.ZOOKEEPER_CLIENT_COULD_NOT_BE_INITIALIZED.getMsg(), ke);
      }
    } catch (Exception e) {
      throw new LockException(
          ErrorMsg.ZOOKEEPER_CLIENT_COULD_NOT_BE_INITIALIZED.getMsg(), e);
    }
  }

  /**
   * Refresh to enable new configurations.
   */
  @Override
  public void refresh() {
    HiveConf conf = ctx.getConf();

    this.sleepTime =
        conf.getTimeVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES,
            TimeUnit.MILLISECONDS);

    this.numRetriesForLock =
        conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES);
  }

  /**
   * Acquire all the locks. Release all the locks and return null if any lock
   * could not be acquired.
   *
   * @param lockObjects List of objects and the modes of the locks requested
   * @param keepAlive Whether the lock is to be persisted after the statement
   * @param lDrvState The state of the driver
   *
   * @throws LockException if the DriverState is set to 'aborted' during the
   *           process of creating all the locks
   */
  @Override
  public List<HiveLock> lock(final List<HiveLockObj> lockObjects,
      final boolean keepAlive, final LockedDriverState lDrvState)
      throws LockException {

    final List<HiveLockObj> lockObjectList = new ArrayList<>(lockObjects);

    Collections.sort(lockObjectList, new Comparator<HiveLockObj>() {
      @Override
      public int compare(HiveLockObj o1, HiveLockObj o2) {
        int cmp = o1.getName().compareTo(o2.getName());
        if (cmp == 0) {
          cmp = o2.getMode().compareTo(o1.getMode());
        }
        return cmp;
      }
    });

    // Walk the list and acquire the locks. If any lock cannot be acquired,
    // release all locks.
    final List<HiveLock> hiveLocks = new ArrayList<>();
    final Set<String> lockedNames = new HashSet<>();

    for (final HiveLockObj lockObject : lockObjectList) {
      LOG.trace("If applicable, creating lock: {}", lockObject);

      // There may be times where an EXCLUSIVE and a SHARED lock will exist on
      // the same object. In this case, only apply the first lock. Since the
      // locks are sorted, with EXCLUSIVE first, only exclusive locks lock.
      if (!lockedNames.add(lockObject.getName())) {
        continue;
      }

      if (lDrvState != null) {
        final boolean isInterrupted;
        lDrvState.stateLock.lock();
        try {
          isInterrupted = lDrvState.isAborted();
        } finally {
          lDrvState.stateLock.unlock();
        }
        if (isInterrupted) {
          releaseLocks(hiveLocks);
          throw new LockException(ErrorMsg.LOCK_ACQUIRE_CANCELLED.getMsg());
        }
      }

      final HiveLock lock;
      try {
        lock = lockRetry(lockObject.getObj(), lockObject.getMode(), keepAlive);
      } catch (LockException e) {
        console.printError("Error in acquire locks");
        LOG.error("Error in acquire locks", e);
        releaseLocks(hiveLocks);
        return null;
      } catch (InterruptedException ie) {
        releaseLocks(hiveLocks);
        Thread.currentThread().interrupt();
        throw new LockException(ErrorMsg.LOCK_ACQUIRE_CANCELLED.getMsg(), ie);
      }

      if (lock != null) {
        hiveLocks.add(lock);
      } else {
        releaseLocks(hiveLocks);
        return null;
      }
    }

    return hiveLocks;
  }

  /**
   * Release all the locks specified. If some of the locks have already been
   * released, ignore them.
   *
   * @param hiveLocks list of hive locks to be released.
   */
  @Override
  public void releaseLocks(final List<HiveLock> hiveLocks) {
    LOG.debug("Releasing locks: {}", hiveLocks);
    if (CollectionUtils.isNotEmpty(hiveLocks)) {
      ListIterator<HiveLock> it = hiveLocks.listIterator(hiveLocks.size());
      while (it.hasPrevious()) {
        final HiveLock hiveLock = it.previous();
        try {
          LOG.trace("About to release lock: {}", hiveLock);
          unlock(hiveLock);
        } catch (LockException e) {
          // The lock may have been released. Ignore and continue
          LOG.warn("Error when releasing lock", e);
        }
      }
    }
  }

  /**
   * @param key The object to be locked
   * @param mode The mode of the lock
   * @param keepAlive Whether the lock is to be persisted after the statement
   *          Acquire the lock. Return null if a conflicting lock is present.
   */
  @Override
  public HiveLock lock(HiveLockObject key, HiveLockMode mode, boolean keepAlive)
      throws LockException {
    try {
      return lockRetry(key, mode, keepAlive);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new LockException(ErrorMsg.LOCK_ACQUIRE_CANCELLED.getMsg(), ie);
    }
  }

  /**
   * Given an absolute ZNode path and a lock mode, return an absolute ZNode path
   * of the new lock node.
   *
   * @param path The path of the root ZNode to create the lock under
   * @param mode The lock mode
   * @return an absolute ZNode path of the new lock node
   */
  private String getLockName(final String path, final HiveLockMode mode) {
    return ZKPaths.makePath(path, "LOCK-" + mode + '-');
  }

  /**
   * Try to obtain a lock. Retry several times if the lock cannot be obtained
   * for any reason.
   *
   * @return null if the lock cannot be obtained because a pre-existing lock
   *         blocks it.
   */
  private ZooKeeperHiveLock lockRetry(final HiveLockObject key,
      final HiveLockMode mode, final boolean keepAlive)
      throws LockException, InterruptedException {

    LOG.debug("Acquiring lock for {} with mode {} {}", key.getName(), mode,
        key.getData().getLockMode());

    final Set<String> conflictingLocks = new HashSet<>();

    ZooKeeperHiveLock lock = null;
    Exception lastException = null;
    int tryNum = 0;

    do {
      lastException = null;
      tryNum++;
      try {
        if (tryNum > 1) {
          Thread.sleep(sleepTime);
          prepareRetry();
        }
        lock = lockPrimitive(key, mode, keepAlive, conflictingLocks);
      } catch (KeeperException ke) {
        lastException = ke;
        switch (ke.code()) {
        case CONNECTIONLOSS:
        case OPERATIONTIMEOUT:
        case NONODE:
        case NODEEXISTS:
          LOG.debug("Possibly transient ZooKeeper exception", ke);
          break;
        default:
          LOG.error("Serious Zookeeper exception", ke);
          break;
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw ie;
      } catch (Exception e) {
        lastException = e;
        LOG.error("Other unexpected exception", e);
      }
    } while (tryNum < numRetriesForLock && lock == null);

    if (lock == null) {
      console.printError("Unable to acquire " + key.getData().getLockMode()
          + ", " + mode + " lock " + key.getDisplayName() + " after " + tryNum
          + " attempts.");

      if (lastException != null) {
        LOG.error("Exceeds maximum retries with errors", lastException);
        throw new LockException(lastException);
      }

      printConflictingLocks(key, mode, conflictingLocks);
    }

    return lock;
  }

  private void printConflictingLocks(HiveLockObject key, HiveLockMode mode,
      Set<String> conflictingLocks) {
    if (!conflictingLocks.isEmpty()) {
      HiveLockObjectData requestedLock =
          new HiveLockObjectData(key.getData().toString());
      LOG.debug("Requested lock " + key.getDisplayName() + ":: mode:"
          + requestedLock.getLockMode() + "," + mode + "; query:"
          + requestedLock.getQueryStr());
      for (String conflictingLock : conflictingLocks) {
        HiveLockObjectData conflictingLockData =
            new HiveLockObjectData(conflictingLock);
        LOG.debug("Conflicting lock to " + key.getDisplayName() + ":: mode:"
            + conflictingLockData.getLockMode() + ";query:"
            + conflictingLockData.getQueryStr() + ";queryId:"
            + conflictingLockData.getQueryId() + ";clientIp:"
            + conflictingLockData.getClientIp());
      }
    }
  }

  /**
   * Creates a primitive lock object on ZooKeeper.
   *
   * @param key The lock data
   * @param mode The lock mode
   * @param keepAlive If true creating PERSISTENT ZooKeeper locks, otherwise
   *          EPHEMERAL ZooKeeper locks
   * @param conflictingLocks The set where we should collect the conflicting
   *          locks when the logging level is set to DEBUG
   * @return The created ZooKeeperHiveLock object, null if there was a
   *         conflicting lock
   * @throws Exception If there was an unexpected Exception
   */
  private ZooKeeperHiveLock lockPrimitive(final HiveLockObject key,
      final HiveLockMode mode, final boolean keepAlive,
      final Collection<String> conflictingLocks) throws Exception {

    HiveLockObjectData lockData = key.getData();
    lockData.setClientIp(clientIP);

    final String rootLockPath =
        ZKPaths.makePath(this.zkNamespace, "", key.getPaths());
    final String lockPatch = getLockName(rootLockPath, mode);

    final String finalLockPath = curatorFramework.create()
        .creatingParentContainersIfNeeded().withProtection()
        .withMode(keepAlive ? CreateMode.PERSISTENT_SEQUENTIAL
            : CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(lockPatch,
            key.getData().toString().getBytes(StandardCharsets.UTF_8));

    final int seqNo = extractSequenceNumber(finalLockPath);
    if (seqNo < 0) {
      curatorFramework.delete().guaranteed().forPath(finalLockPath);
      throw new LockException(
          "The created node does not contain a sequence number: "
              + finalLockPath);
    }

    final PathAndNode pathAndNode = ZKPaths.getPathAndNode(finalLockPath);
    List<String> children = Collections.emptyList();
    try {
      children = curatorFramework.getChildren().forPath(pathAndNode.getPath());
    } catch (Exception e) {
      curatorFramework.delete().guaranteed().forPath(finalLockPath);
      ThreadUtils.checkInterrupted(e);
      throw e;
    }

    final Set<String> blockingLocks = new HashSet<>(0);
    for (final String lockNode : children) {
      final String currentLockPath =
          ZKPaths.makePath(pathAndNode.getPath(), lockNode);

      final int existingSeqNum = extractSequenceNumber(currentLockPath);

      // Check if this is an existing lock with lower sequence number
      if (existingSeqNum >= 0 && existingSeqNum < seqNo) {
        switch (mode) {
        case SHARED:
          final HiveLockMode currentLockMode = getLockMode(currentLockPath);
          if (HiveLockMode.SHARED == currentLockMode) {
            // Ignore if the discovered lock mode is also SHARED
            // Otherwise fall through and add to blocking locks list
            break;
          }
        case EXCLUSIVE:
          // Cannot grab exclusive lock if there already exists a lock
        default:
          blockingLocks.add(currentLockPath);
          break;
        }
      }
    }

    if (!blockingLocks.isEmpty()) {
      // delete the newly created lock - will have to try again later
      curatorFramework.delete().guaranteed().forPath(finalLockPath);

      if (LOG.isDebugEnabled()) {
        for (final String blockingLockPath : blockingLocks) {
          try {
            final String dataStr =
                new String(curatorFramework.getData().forPath(blockingLockPath),
                    StandardCharsets.UTF_8);
            conflictingLocks.add(dataStr);
          } catch (Exception e) {
            LOG.debug("Could not get data for node: {}", blockingLockPath, e);
          }
        }
      }

      LOG.info("Cannot obtain lock. Blocked by existing lock.");
      LOG.debug("Blocked by {}", blockingLocks);
      return null;
    }

    final Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      try {
        switch (mode) {
        case EXCLUSIVE:
          metrics
              .incrementCounter(MetricsConstant.ZOOKEEPER_HIVE_EXCLUSIVELOCKS);
          break;
        case SEMI_SHARED:
          metrics
              .incrementCounter(MetricsConstant.ZOOKEEPER_HIVE_SEMISHAREDLOCKS);
          break;
        case SHARED:
          metrics.incrementCounter(MetricsConstant.ZOOKEEPER_HIVE_SHAREDLOCKS);
          break;
        default:
          break;
        }
      } catch (Exception e) {
        LOG.warn("Error Reporting hive client zookeeper lock operation "
            + "to Metrics system", e);
      }
    }

    return new ZooKeeperHiveLock(finalLockPath, key, mode);
  }

  // Hardcoded in {@link org.apache.zookeeper.server.PrepRequestProcessor}
  static final int SEQUENTIAL_SUFFIX_DIGITS = 10;

  /**
   * Extracts the ten-digit suffix from a sequential znode path.
   *
   * @param path the path of a sequential znodes
   * @return the sequence number
   */
  private int extractSequenceNumber(final String path) {
    final int length = path.length();
    if (length >= SEQUENTIAL_SUFFIX_DIGITS) {
      final String seqStr = path.substring(length - SEQUENTIAL_SUFFIX_DIGITS);
      try {
        return Integer.parseInt(seqStr);
      } catch (Exception e) {
      }
    }
    // invalid number
    return -1;
  }

  /**
   * Remove the lock specified.
   */
  @Override
  public void unlock(HiveLock hiveLock) throws LockException {
    unlockPrimitive(hiveLock, zkNamespace, curatorFramework);
  }

  /**
   * Remove the lock specified. Ignore any {@link InterruptedException} and
   * complete the lock. Any locks not deleted may block future queries.
   */
  @VisibleForTesting
  static void unlockPrimitive(HiveLock hiveLock, String parent,
      CuratorFramework curatorFramework) throws LockException {

    Objects.requireNonNull(hiveLock, "Lock must not be null");

    final ZooKeeperHiveLock zLock = (ZooKeeperHiveLock) hiveLock;

    try {
      curatorFramework.delete().guaranteed().forPath(zLock.getPath());
    } catch (Exception e) {
      ThreadUtils.checkInterrupted(e);
      throw new LockException(e);
    } finally {
      final Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        final HiveLockMode lMode = hiveLock.getHiveLockMode();
        try {
          switch (lMode) {
          case EXCLUSIVE:
            metrics.decrementCounter(
                MetricsConstant.ZOOKEEPER_HIVE_EXCLUSIVELOCKS);
            break;
          case SEMI_SHARED:
            metrics.decrementCounter(
                MetricsConstant.ZOOKEEPER_HIVE_SEMISHAREDLOCKS);
            break;
          case SHARED:
            metrics
                .decrementCounter(MetricsConstant.ZOOKEEPER_HIVE_SHAREDLOCKS);
            break;
          default:
            break;
          }
        } catch (Exception e) {
          LOG.warn("Error Reporting hive client zookeeper unlock operation "
              + "to Metrics system", e);
        }
      }
    }

    try {
      final PathAndNode pathAndNode = ZKPaths.getPathAndNode(zLock.getPath());
      final String lockParentPath = pathAndNode.getPath();
      // Delete the parent node if all the children have been deleted
      final Stat stat = curatorFramework.checkExists().forPath(lockParentPath);
      if (stat != null && stat.getNumChildren() == 0) {
        curatorFramework.delete().forPath(lockParentPath);
      }
    } catch (Exception e) {
      // This clean up is "nice to have" do not exit on error here
      ThreadUtils.checkInterrupted(e);
    }
  }

  /**
   * Release all locks - including PERSISTENT locks.
   */
  public static void releaseAllLocks(final HiveConf conf) throws Exception {
    final String nameSpace =
        conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_NAMESPACE);
    Preconditions.checkState(nameSpace != null);

    final CuratorFramework curatorFramework =
        CuratorFrameworkSingleton.getInstance(conf);

    final String namespacePath = ZKPaths.makePath(nameSpace, "");

    try {
      LOG.debug("Deleting entire Hive lock namespace: {}", namespacePath);
      curatorFramework.delete().deletingChildrenIfNeeded()
          .forPath(namespacePath);
    } catch (KeeperException ke) {
      if (ke.code() == Code.NONODE) {
        LOG.debug("Namespace node not present");
      } else {
        throw ke;
      }
    }
  }

  /**
   * Get all locks.
   */
  @Override
  public List<HiveLock> getLocks(boolean verifyTablePartition,
      boolean fetchData) throws LockException {
    return getLocks(ctx.getConf(), null, zkNamespace, verifyTablePartition,
        fetchData);
  }

  /**
   * Get all locks for a particular object.
   */
  @Override
  public List<HiveLock> getLocks(HiveLockObject key,
      boolean verifyTablePartitions, boolean fetchData) throws LockException {
    return getLocks(ctx.getConf(), key, zkNamespace, verifyTablePartitions,
        fetchData);
  }

  /**
   * @param conf Hive configuration
   * @param key The object to be compared against - if key is null, then get all
   *          locks
   **/
  private List<HiveLock> getLocks(HiveConf conf, HiveLockObject key,
      String parent, boolean verifyTablePartition, boolean fetchData)
      throws LockException {
    final List<HiveLock> locks = new ArrayList<>();
    final String commonParent;
    List<String> children = Collections.emptyList();
    boolean recurse = true;

    try {
      if (key != null) {
        commonParent = ZKPaths.makePath(parent, key.getName());
        recurse = false;
      } else {
        commonParent = ZKPaths.makePath(parent, "");
      }
      children = curatorFramework.getChildren().forPath(commonParent);
    } catch (Exception e) {
      // no locks present
      return locks;
    }

    final Queue<String> childn = new ArrayDeque<>();
    for (String child : children) {
      childn.add(ZKPaths.makePath(commonParent, child));
    }

    while (!childn.isEmpty()) {
      String curChild = childn.poll();

      if (recurse) {
        try {
          children = curatorFramework.getChildren().forPath(curChild);
          for (String child : children) {
            childn.add(ZKPaths.makePath(curChild, child));
          }
        } catch (Exception e) {
          LOG.debug("Exception while getting locks for: {}", curChild, e);
          // nothing to do
        }
      }

      final HiveLockMode mode = getLockMode(curChild);
      if (mode == null) {
        continue;
      }

      HiveLockObjectData data = null;

      // set the lock object with a dummy data, and then do a set if needed.
      HiveLockObject obj = getLockObject(conf, curChild, mode, data, parent,
          verifyTablePartition);
      if (obj == null) {
        continue;
      }

      if ((key == null) || (obj.getName().equals(key.getName()))) {

        if (fetchData) {
          try {
            data = new HiveLockObjectData(
                new String(curatorFramework.getData().forPath(curChild),
                    StandardCharsets.UTF_8));
            data.setClientIp(clientIP);
          } catch (Exception e) {
            LOG.warn("Error in getting data for {}", curChild, e);
            // ignore error
          }
        }
        obj.setData(data);
        locks.add(new ZooKeeperHiveLock(curChild, obj, mode));
      }
    }
    return locks;
  }

  /**
   * Remove all redundant nodes.
   */
  private void removeAllRedundantNodes() {
    try {
      checkRedundantNode(ZKPaths.makePath(zkNamespace, ""));
    } catch (Exception e) {
      LOG.warn("Exception while removing all redundant nodes", e);
    }
  }

  private void checkRedundantNode(String node) {
    try {
      // Nothing to do if it is a lock mode
      if (getLockMode(node) != null) {
        return;
      }

      List<String> children = curatorFramework.getChildren().forPath(node);
      for (String child : children) {
        checkRedundantNode(ZKPaths.makePath(node, child));
      }

      final Stat stat = curatorFramework.checkExists().forPath(node);
      if (stat != null && stat.getNumChildren() == 0) {
        curatorFramework.delete().forPath(node);
      }
    } catch (Exception e) {
      LOG.warn("Error in checkRedundantNode for node {}", node, e);
    }
  }

  /**
   * Release all transient locks, by simply closing the client.
   */
  @Override
  public void close() throws LockException {
    try {
      if (HiveConf.getBoolVar(ctx.getConf(),
          HiveConf.ConfVars.HIVE_ZOOKEEPER_CLEAN_EXTRA_NODES)) {
        removeAllRedundantNodes();
      }
    } catch (Exception e) {
      LOG.error("Failed to close zooKeeper client", e);
      throw new LockException(e);
    }
  }

  /**
   * Get the object from the path of the lock. The object may correspond to a
   * table, a partition or a parent to a partition. For eg: if Table T is
   * partitioned by ds, hr and ds=1/hr=1 is a valid partition, the lock may also
   * correspond to T@ds=1, which is not a valid object
   **/
  private static HiveLockObject getLockObject(HiveConf conf, String path,
      HiveLockMode mode, HiveLockObjectData data, String parent,
      boolean verifyTablePartition) throws LockException {
    try {
      final Hive db = Hive.get(conf);
      final PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
      final String objectPath = StringUtils.removeStart(pathAndNode.getPath(),
          ZKPaths.makePath(parent, ""));
      final String[] names = ZKPaths.split(objectPath).toArray(new String[0]);

      if (names.length < 2) {
        return null;
      }

      if (!verifyTablePartition) {
        return new HiveLockObject(names, data);
      }

      // do not throw exception if table does not exist
      Table tab = db.getTable(names[0], names[1], false);
      if (tab == null) {
        return null;
      }

      if (names.length == 2) {
        return new HiveLockObject(tab, data);
      }

      Map<String, String> partSpec = new HashMap<>();
      for (int indx = 2; indx < names.length; indx++) {
        String[] partVals = names[indx].split("=");
        partSpec.put(partVals[0], partVals[1]);
      }

      Partition partn;
      try {
        partn = db.getPartition(tab, partSpec, false);
      } catch (HiveException e) {
        partn = null;
      }

      if (partn == null) {
        partn = new DummyPartition(tab, path, partSpec);
      }

      return new HiveLockObject(partn, data);
    } catch (Exception e) {
      throw new LockException(e);
    }
  }

  private static Pattern shMode = Pattern.compile("^.*LOCK-(SHARED)-([0-9]+)$");
  private static Pattern exMode = Pattern.compile("^.*LOCK-(EXCLUSIVE)-([0-9]+)$");

  /* Get the mode of the lock encoded in the path */
  private static HiveLockMode getLockMode(String path) {

    if (shMode.matcher(path).matches()) {
      return HiveLockMode.SHARED;
    }

    if (exMode.matcher(path).matches()) {
      return HiveLockMode.EXCLUSIVE;
    }

    return null;
  }

  @Override
  public void prepareRetry() throws LockException {
  }
}
