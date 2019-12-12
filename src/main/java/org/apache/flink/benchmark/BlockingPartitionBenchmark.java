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

import org.apache.flink.benchmark.functions.LongSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

/**
 * JMH throughput benchmark runner.
 */
@OperationsPerInvocation(value = BlockingPartitionBenchmark.RECORDS_PER_INVOCATION)
public class BlockingPartitionBenchmark extends BenchmarkBase {

	public static final int RECORDS_PER_INVOCATION = 15_000_000;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + BlockingPartitionBenchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public void uncompressedFilePartition(UncompressedFileEnvironmentContext context) throws Exception {
		executeBenchmark(context.env);
	}

	@Benchmark
	public void compressedFilePartition(CompressedFileEnvironmentContext context) throws Exception {
		executeBenchmark(context.env);
	}

	@Benchmark
	public void uncompressedMmapPartition(UncompressedMmapEnvironmentContext context) throws Exception {
		executeBenchmark(context.env);
	}

	private void executeBenchmark(StreamExecutionEnvironment env) throws Exception {
		DataStreamSource<Long> source = env.addSource(new LongSource(RECORDS_PER_INVOCATION));
		source.addSink(new DiscardingSink<>());

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setChaining(false);
		streamGraph.setBlockingConnectionsBetweenChains(true);
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);

		env.execute(streamGraph);
	}

	/**
	 * Setup for the benchmark(s).
	 */
	public static class BlockingPartitionEnvironmentContext extends FlinkEnvironmentContext {
		@Setup
		public void setUp() throws IOException {
			super.setUp();

			env.setBufferTimeout(-1);
		}

		protected Configuration createConfiguration(boolean compressionEnabled, String subpartitionType) {
			Configuration configuration = super.createConfiguration();

			configuration.setBoolean(NettyShuffleEnvironmentOptions.BLOCKING_SHUFFLE_COMPRESSION_ENABLED, compressionEnabled);
			configuration.setString(NettyShuffleEnvironmentOptions.NETWORK_BLOCKING_SHUFFLE_TYPE, subpartitionType);
			return configuration;
		}
	}

	public static class UncompressedFileEnvironmentContext extends BlockingPartitionEnvironmentContext {
		@Override
		protected Configuration createConfiguration() {
			return createConfiguration(false, "file");
		}
	}

	public static class CompressedFileEnvironmentContext extends BlockingPartitionEnvironmentContext {
		@Override
		protected Configuration createConfiguration() {
			return createConfiguration(true, "file");
		}
	}

	public static class UncompressedMmapEnvironmentContext extends BlockingPartitionEnvironmentContext {
		@Override
		protected Configuration createConfiguration() {
			return createConfiguration(false, "mmap");
		}
	}
}
