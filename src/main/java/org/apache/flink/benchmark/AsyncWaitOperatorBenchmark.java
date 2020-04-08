/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark;

import org.apache.flink.benchmark.functions.LongSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@OperationsPerInvocation(value = AsyncWaitOperatorBenchmark.RECORDS_PER_INVOCATION)
public class AsyncWaitOperatorBenchmark extends BenchmarkBase {
	public static final int RECORDS_PER_INVOCATION = 1_000_000;

	private static final long CHECKPOINT_INTERVAL_MS = 100;

	private static ExecutorService executor;

	@Param
	public AsyncDataStream.OutputMode outputMode;

	public static void main(String[] args)
		throws RunnerException {
		Options options = new OptionsBuilder()
			.verbosity(VerboseMode.NORMAL)
			.include(".*" + AsyncWaitOperatorBenchmark.class.getCanonicalName() + ".*")
			.build();

		new Runner(options).run();
	}

	@Setup
	public void setUp() {
		executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}

	@TearDown
	public void tearDown() {
		executor.shutdown();
	}

	@Benchmark
	public void asyncWait(FlinkEnvironmentContext context) throws Exception {

		StreamExecutionEnvironment env = context.env;
		env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
		env.setParallelism(1);

		DataStreamSource<Long> source = env.addSource(new LongSource(RECORDS_PER_INVOCATION));
		DataStream<Long> result = createAsyncOperator(source);
		result.addSink(new DiscardingSink<>());

		env.execute();
	}

	private DataStream<Long> createAsyncOperator(DataStreamSource<Long> source) {
		switch (outputMode) {
			case ORDERED:
				return AsyncDataStream.orderedWait(
						source,
						new BenchmarkAsyncFunctionExecutor(),
						0,
						TimeUnit.MILLISECONDS);
			case UNORDERED:
				return AsyncDataStream.unorderedWait(
						source,
						new BenchmarkAsyncFunctionExecutor(),
						0,
						TimeUnit.MILLISECONDS);
			default:
				throw new UnsupportedOperationException("Unknown mode");
		}
	}

	private static class BenchmarkAsyncFunctionExecutor extends RichAsyncFunction<Long, Long> {
		@Override
		public void asyncInvoke(Long input, ResultFuture<Long> resultFuture) {
			executor.execute(() -> resultFuture.complete(Collections.singleton(input * 2)));
		}
	}
}
