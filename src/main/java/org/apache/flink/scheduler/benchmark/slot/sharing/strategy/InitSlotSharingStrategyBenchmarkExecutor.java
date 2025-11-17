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

package org.apache.flink.scheduler.benchmark.slot.sharing.strategy;

import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.SlotSharingStrategy;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.JobVertexConnectionUtils;
import org.apache.flink.scheduler.benchmark.SchedulerBenchmarkExecutorBase;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Arrays;
import java.util.Collection;

/**
 * The executor to run {@link InitSlotSharingStrategyBenchmark} for initializing {@link
 * SlotSharingStrategy}.
 */
public class InitSlotSharingStrategyBenchmarkExecutor extends SchedulerBenchmarkExecutorBase {

    public static final Collection<JobVertex> JOB_VERTICES;

    static JobVertex newJobVertex(int parallelism) {
        JobVertex jobVertex = new JobVertex("", new JobVertexID());
        jobVertex.setParallelism(parallelism);
        jobVertex.setInvokableClass(NoOpInvokable.class);
        return jobVertex;
    }

    static {
        JobVertex jobVertexA = newJobVertex(1000);
        JobVertex jobVertexB = newJobVertex(4000);
        JobVertex jobVertexC = newJobVertex(4000);
        JobVertex jobVertexD = newJobVertex(2000);
        JobVertex jobVertexE = newJobVertex(3000);
        JobVertex jobVertexF = newJobVertex(1000);

        JobVertexConnectionUtils.connectNewDataSetAsInput(
                jobVertexB,
                jobVertexA,
                DistributionPattern.ALL_TO_ALL,
                ResultPartitionType.BLOCKING);
        JobVertexConnectionUtils.connectNewDataSetAsInput(
                jobVertexC,
                jobVertexB,
                DistributionPattern.POINTWISE,
                ResultPartitionType.BLOCKING);
        JobVertexConnectionUtils.connectNewDataSetAsInput(
                jobVertexD,
                jobVertexC,
                DistributionPattern.ALL_TO_ALL,
                ResultPartitionType.BLOCKING);
        JobVertexConnectionUtils.connectNewDataSetAsInput(
                jobVertexE,
                jobVertexD,
                DistributionPattern.ALL_TO_ALL,
                ResultPartitionType.BLOCKING);

        JOB_VERTICES =
                Arrays.asList(
                        jobVertexA, jobVertexB, jobVertexC, jobVertexD, jobVertexE, jobVertexF);
    }

    @Param({"NONE", "TASKS"})
    private TaskManagerLoadBalanceMode taskManagerLoadBalanceMode;

    private InitSlotSharingStrategyBenchmark benchmark;

    public static void main(String[] args) throws RunnerException {
        runBenchmark(InitSlotSharingStrategyBenchmark.class);
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        benchmark = new InitSlotSharingStrategyBenchmark(taskManagerLoadBalanceMode, JOB_VERTICES);
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void createSlotSharingStrategy(Blackhole blackhole) {
        blackhole.consume(benchmark.createSlotSharingStrategy());
    }
}
