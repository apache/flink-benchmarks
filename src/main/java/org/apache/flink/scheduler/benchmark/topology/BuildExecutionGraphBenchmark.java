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

package org.apache.flink.scheduler.benchmark.topology;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.scheduler.benchmark.JobConfiguration;
import org.apache.flink.scheduler.benchmark.SchedulerBenchmarkBase;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;

import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.scheduler.benchmark.SchedulerBenchmarkUtils.createJobGraph;

/**
 * The benchmark of building the topology of {@link ExecutionGraph} in a STREAMING/BATCH job.
 * The related method is {@link ExecutionGraph#attachJobGraph},
 */
public class BuildExecutionGraphBenchmark extends SchedulerBenchmarkBase {

	private List<JobVertex> jobVertices;
	private ExecutionGraph executionGraph;

	@Param({"BATCH", "STREAMING"})
	private JobConfiguration jobConfiguration;

	public static void main(String[] args) throws RunnerException {
		runBenchmark(BuildExecutionGraphBenchmark.class);
	}

	@Setup(Level.Trial)
	public void setup() throws Exception {
		jobVertices = createDefaultJobVertices(jobConfiguration);
		final JobGraph jobGraph = createJobGraph(jobConfiguration);
		executionGraph = TestingExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();
	}

	@Benchmark
	@BenchmarkMode(Mode.SingleShotTime)
	public void buildTopology() throws Exception {
		executionGraph.attachJobGraph(jobVertices);
	}
}
