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

package org.apache.flink.scheduler.benchmark.scheduling;

import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.benchmark.scheduling.SchedulingDownstreamTasksInBatchJobBenchmark;
import org.apache.flink.scheduler.benchmark.SchedulerBenchmarkExecutorBase;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.RunnerException;

/** The benchmark of scheduling downstream task in a BATCH job. */
public class SchedulingDownstreamTasksInBatchJobBenchmarkExecutor
        extends SchedulerBenchmarkExecutorBase {

    @Param({"BATCH", "BATCH_HYBRID_DEFAULT", "BATCH_HYBRID_PARTIAL_FINISHED", "BATCH_HYBRID_ALL_FINISHED"})
    private JobConfiguration jobConfiguration;

    private SchedulingDownstreamTasksInBatchJobBenchmark benchmark;

    public static void main(String[] args) throws RunnerException {
        runBenchmark(SchedulingDownstreamTasksInBatchJobBenchmarkExecutor.class);
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        benchmark = new SchedulingDownstreamTasksInBatchJobBenchmark();
        benchmark.setup(jobConfiguration);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void schedulingDownstreamTasks() {
        benchmark.schedulingDownstreamTasks();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        benchmark.teardown();
    }
}
