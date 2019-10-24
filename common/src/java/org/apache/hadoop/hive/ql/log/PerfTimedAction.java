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

/**
 * An enumeration of all the measured actions.
 */
public enum PerfTimedAction {

  ACQUIRE_READ_WRITE_LOCKS("acquireReadWriteLocks"),
  COMPILE("compile"),
  WAIT_COMPILE("waitCompile"),
  PARSE("parse"),
  ANALYZE("semanticAnalyze"),
  OPTIMIZER("optimizer"),
  MATERIALIZED_VIEWS_REGISTRY_REFRESH("MaterializedViewsRegistryRefresh"),
  DO_AUTHORIZATION("doAuthorization"),
  DRIVER_EXECUTE("Driver.execute"),
  INPUT_SUMMARY("getInputSummary"),
  INPUT_PATHS("getInputPaths"),
  GET_SPLITS("getSplits"),
  RUN_TASKS("runTasks"),
  SERIALIZE_PLAN("serializePlan"),
  DESERIALIZE_PLAN("deserializePlan"),
  CLONE_PLAN("clonePlan"),
  RENAME_FILE("renameFile"),
  REMOVE_TMP_DUP_FILES("removeTempOrDuplicateFiles"),
  MOVE_FILE_STATUS("moveSpecifiedFileStatus"),
  RENAME_MOVE_FILES("RenameOrMoveFiles"),
  CREATE_EMPTY_BUCKETS("createEmptyBuckets"),
  RELEASE_LOCKS("releaseLocks"),
  PRUNE_LISTING("prune-listing"),
  PARTITION_RETRIEVING("partition-retrieving"),
  PRE_HOOK("PreHook"),
  POST_HOOK("PostHook"),
  FAILURE_HOOK("FailureHook"),
  TEZ_COMPILER("TezCompiler"),
  TEZ_SUBMIT_TO_RUNNING("TezSubmitToRunningDag"),
  TEZ_BUILD_DAG("TezBuildDag"),
  TEZ_SUBMIT_DAG("TezSubmitDag"),
  TEZ_RUN_DAG("TezRunDag"),
  TEZ_CREATE_VERTEX("TezCreateVertex"),
  TEZ_RUN_VERTEX("TezRunVertex"),
  TEZ_INITIALIZE_PROCESSOR("TezInitializeProcessor"),
  TEZ_RUN_PROCESSOR("TezRunProcessor"),
  TEZ_INIT_OPERATORS("TezInitializeOperators"),
  TEZ_GET_SESSION("TezGetSession"),
  LOAD_HASHTABLE("LoadHashtable"),
  SAVE_TO_RESULTS_CACHE("saveToResultsCache"),
  SPARK_SUBMIT_TO_RUNNING("SparkSubmitToRunning"),
  SPARK_BUILD_PLAN("SparkBuildPlan"),
  SPARK_BUILD_RDD_GRAPH("SparkBuildRDDGraph"),
  SPARK_CREATE_EXPLAIN_PLAN("SparkCreateExplainPlan"),
  SPARK_SUBMIT_JOB("SparkSubmitJob"),
  SPARK_RUN_JOB("SparkRunJob"),
  SPARK_CREATE_TRAN("SparkCreateTran"),
  SPARK_RUN_STAGE("SparkRunStage"),
  SPARK_INIT_OPERATORS("SparkInitializeOperators"),
  SPARK_GENERATE_TASK_TREE("SparkGenerateTaskTree"),
  SPARK_OPTIMIZE_OPERATOR_TREE("SparkOptimizeOperatorTree"),
  SPARK_OPTIMIZE_TASK_TREE("SparkOptimizeTaskTree"),
  SPARK_FLUSH_HASHTABLE("SparkFlushHashTable"),
  SPARK_DYNAMICALLY_PRUNE_PARTITIONS("SparkDynamicallyPrunePartitions"),
  FILE_MOVES("FileMoves"),
  LOAD_TABLE("LoadTable"),
  LOAD_PARTITION("LoadPartition"),
  LOAD_DYNAMIC_PARTITIONS("LoadDynamicPartitions");

  private final String name;

  PerfTimedAction(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
