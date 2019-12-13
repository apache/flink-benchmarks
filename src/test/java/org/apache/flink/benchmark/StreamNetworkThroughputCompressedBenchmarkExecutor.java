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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.streaming.runtime.io.benchmark.StreamNetworkCompressionThroughputBenchmark;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * JMH throughput benchmark runner for streaming compressed scenario.
 */
@OperationsPerInvocation(value = StreamNetworkThroughputCompressedBenchmarkExecutor.RECORDS_PER_INVOCATION)
public class StreamNetworkThroughputCompressedBenchmarkExecutor extends BenchmarkBase {

	static final int RECORDS_PER_INVOCATION = 5_000_000;

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
			.verbosity(VerboseMode.NORMAL)
			.include(".*" + StreamNetworkThroughputCompressedBenchmarkExecutor.class.getSimpleName() + ".*")
			.build();

		new Runner(options).run();
	}

	@Benchmark
	public void networkThroughputCompressed(CompressionEnvironment context) throws Exception {
		context.executeBenchmark(RECORDS_PER_INVOCATION);
	}

	/**
	 * Setup for the streaming compressed benchmark(s).
	 */
	@State(Thread)
	public static class CompressionEnvironment extends StreamNetworkCompressionThroughputBenchmark {

		private final int flushTimeout = 100;

		private final int numberOfSubpartitions = 1000;

		private final int numberOfWriters = 4;

		@Setup
		public void setUp() throws Exception {
			final Configuration configuration = new Configuration();
			configuration.setBoolean(NettyShuffleEnvironmentOptions.PIPELINED_SHUFFLE_COMPRESSION_ENABLED, true);

			setUp(numberOfWriters, numberOfSubpartitions, flushTimeout, false, false, -1, -1, configuration);
		}

		@TearDown
		public void tearDown() throws Exception {
			super.tearDown();
		}
	}
}
