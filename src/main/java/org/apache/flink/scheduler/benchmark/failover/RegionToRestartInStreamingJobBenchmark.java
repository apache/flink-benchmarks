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

package org.apache.flink.scheduler.benchmark.failover;

import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.scheduler.benchmark.JobConfiguration;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.switchAllVerticesToRunning;
import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.deployAllTasks;

/**
 * The benchmark of calculating region to restart when failover occurs in a STREAMING job.
 * The related method is {@link RestartPipelinedRegionFailoverStrategy#getTasksNeedingRestart}.
 */
public class RegionToRestartInStreamingJobBenchmark extends FailoverBenchmarkBase {

	@Param("STREAMING")
	private JobConfiguration jobConfiguration;

	public static void main(String[] args) throws RunnerException {
		runBenchmark(RegionToRestartInStreamingJobBenchmark.class);
	}

	@Setup(Level.Trial)
	public void setup() throws Exception {
		createRestartPipelinedRegionFailoverStrategy(jobConfiguration);

		TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

		deployAllTasks(executionGraph, slotBuilder);

		switchAllVerticesToRunning(executionGraph);
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void calculateRegionToRestart(Blackhole blackhole) {
		tasks = strategy.getTasksNeedingRestart(
				executionGraph.getJobVertex(source.getID()).getTaskVertices()[0].getID(),
				new Exception("For test."));
		blackhole.consume(tasks);
	}
}
