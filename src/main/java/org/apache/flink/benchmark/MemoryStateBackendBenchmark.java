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

package org.apache.flink.benchmark;

import org.apache.flink.benchmark.functions.IntLongApplications;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

import static org.openjdk.jmh.annotations.Scope.Thread;

@OperationsPerInvocation(value = MemoryStateBackendBenchmark.RECORDS_PER_INVOCATION)
public class MemoryStateBackendBenchmark extends StateBackendBenchmarkBase {
	public static final int RECORDS_PER_INVOCATION = 7_000_000;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + MemoryStateBackendBenchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public void stateBackends(MemoryStateBackendContext context) throws Exception {
		IntLongApplications.reduceWithWindow(context.source, TumblingEventTimeWindows.of(Time.seconds(10_000)));
		context.execute();
	}

	@State(Thread)
	public static class MemoryStateBackendContext extends StateBackendContext {
		@Param({"MEMORY", "FS", "FS_ASYNC"})
		public StateBackend stateBackend = StateBackend.MEMORY;

		@Setup
		public void setUp() throws IOException {
			super.setUp(stateBackend, RECORDS_PER_INVOCATION);
		}

		@TearDown
		public void tearDown() throws IOException {
			super.tearDown();
		}
	}
}
