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

package org.apache.flink.scheduler.benchmark.deploying;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.benchmark.deploying.DeployingDownstreamTasksInBatchJobBenchmark;
import org.apache.flink.scheduler.benchmark.SchedulerBenchmarkExecutorBase;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.RunnerException;

/**
 * The benchmark of deploying downstream tasks in a BATCH job. The related method is {@link
 * Execution#deploy}.
 */
public class DeployingDownstreamTasksInBatchJobBenchmarkExecutor
        extends SchedulerBenchmarkExecutorBase {

    @Param("BATCH")
    private JobConfiguration jobConfiguration;

    private DeployingDownstreamTasksInBatchJobBenchmark benchmark;

    public static void main(String[] args) throws RunnerException {
        runBenchmark(DeployingDownstreamTasksInBatchJobBenchmarkExecutor.class);
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        benchmark = new DeployingDownstreamTasksInBatchJobBenchmark();
        benchmark.setup(jobConfiguration);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void deployDownstreamTasks() throws Exception {
        benchmark.deployDownstreamTasks();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        benchmark.teardown();
    }
}
