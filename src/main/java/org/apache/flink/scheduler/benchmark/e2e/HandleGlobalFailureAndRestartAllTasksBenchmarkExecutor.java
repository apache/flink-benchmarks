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

package org.apache.flink.scheduler.benchmark.e2e;

import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.benchmark.e2e.HandleGlobalFailureAndRestartAllTasksBenchmark;
import org.apache.flink.scheduler.benchmark.SchedulerBenchmarkExecutorBase;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.RunnerException;

/** The benchmark of handle global failure and restarting tasks in a STREAMING/BATCH job. */
public class HandleGlobalFailureAndRestartAllTasksBenchmarkExecutor extends SchedulerBenchmarkExecutorBase {

    @Param({"BATCH", "STREAMING", "BATCH_EVENLY", "STREAMING_EVENLY"})
    private JobConfiguration jobConfiguration;

    private HandleGlobalFailureAndRestartAllTasksBenchmark benchmark;

    public static void main(String[] args) throws RunnerException {
        runBenchmark(HandleGlobalFailureAndRestartAllTasksBenchmarkExecutor.class);
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        benchmark = new HandleGlobalFailureAndRestartAllTasksBenchmark();
        benchmark.setup(jobConfiguration);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void handleGlobalFailureAndRestartAllTasks() throws Exception {
        benchmark.handleGlobalFailureAndRestartAllTasks();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        benchmark.teardown();
    }
}
