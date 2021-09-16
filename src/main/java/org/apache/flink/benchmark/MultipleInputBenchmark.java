/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.benchmark.functions.LongSource;
import org.apache.flink.benchmark.functions.QueuingLongSource;
import org.apache.flink.benchmark.operators.MultiplyByTwoOperatorFactory;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.CompletableFuture;

public class MultipleInputBenchmark extends BenchmarkBase {
	public static final int RECORDS_PER_INVOCATION = TwoInputBenchmark.RECORDS_PER_INVOCATION;
	public static final int ONE_IDLE_RECORDS_PER_INVOCATION = TwoInputBenchmark.ONE_IDLE_RECORDS_PER_INVOCATION;
	public static final long CHECKPOINT_INTERVAL_MS = TwoInputBenchmark.CHECKPOINT_INTERVAL_MS;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + MultipleInputBenchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	@OperationsPerInvocation(RECORDS_PER_INVOCATION)
	public void multiInputMapSink(FlinkEnvironmentContext context) throws Exception {

		StreamExecutionEnvironment env = context.env;
		env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

		long numRecordsPerInput = RECORDS_PER_INVOCATION / 2;
		DataStreamSource<Long> source1 = env.addSource(new LongSource(numRecordsPerInput));
		DataStreamSource<Long> source2 = env.addSource(new LongSource(numRecordsPerInput));
		connectAndDiscard(env, source1, source2);

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(ONE_IDLE_RECORDS_PER_INVOCATION)
	public void multiInputOneIdleMapSink(FlinkEnvironmentContext context) throws Exception {

		StreamExecutionEnvironment env = context.env;
		env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

		QueuingLongSource.reset();
		DataStreamSource<Long> source1 = env.addSource(new QueuingLongSource(1, ONE_IDLE_RECORDS_PER_INVOCATION - 1));
		DataStreamSource<Long> source2 = env.addSource(new QueuingLongSource(2, 1));
		connectAndDiscard(env, source1, source2);

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(RECORDS_PER_INVOCATION)
	public void multiInputChainedIdleSource(FlinkEnvironmentContext context) throws Exception {
		final StreamExecutionEnvironment env = context.env;
		env.getConfig().enableObjectReuse();

		final DataStream<Long> source1 =
				env.fromSource(
						new NumberSequenceSource(1L, RECORDS_PER_INVOCATION),
						WatermarkStrategy.noWatermarks(),
						"source-1");

		final DataStreamSource<Integer> source2 =
				env.fromSource(new IdlingSource(1), WatermarkStrategy.noWatermarks(), "source-2");

		MultipleInputTransformation<Long> transform = new MultipleInputTransformation<>(
				"custom operator",
				new MultiplyByTwoOperatorFactory(),
				BasicTypeInfo.LONG_TYPE_INFO,
				1);

		transform.addInput(((DataStream<?>) source1).getTransformation());
		transform.addInput(((DataStream<?>) source2).getTransformation());
		transform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);

		env.addOperator(transform);
		new MultipleConnectedStreams(env).transform(transform).addSink(new SinkClosingIdlingSource()).setParallelism(1);
		context.execute();
	}

	private static class IdlingSource extends MockSource {
		private static CompletableFuture<Void> canFinish = new CompletableFuture<>();

		public static void signalCanFinish() {
			canFinish.complete(null);
		}

		public static void reset() {
			canFinish.completeExceptionally(new IllegalStateException("State has been reset"));
			canFinish = new CompletableFuture<>();
		}

		public IdlingSource(int numSplits) {
			super(Boundedness.BOUNDED, numSplits, true, true);
		}

		@Override
		public SourceReader<Integer, MockSourceSplit> createReader(
				SourceReaderContext readerContext) {
			return new MockSourceReader(true, true) {
				@Override
				public InputStatus pollNext(ReaderOutput<Integer> sourceOutput) {
					if (canFinish.isDone() && !canFinish.isCompletedExceptionally()) {
						return InputStatus.END_OF_INPUT;
					} else {
						return InputStatus.NOTHING_AVAILABLE;
					}
				}

				@Override
				public synchronized CompletableFuture<Void> isAvailable() {
					return canFinish;
				}
			};
		}
	}

	private static class SinkClosingIdlingSource implements SinkFunction<Long> {
		private int recordsSoFar = 0;

		@Override
		public void invoke(Long value) {
			if (++recordsSoFar >= RECORDS_PER_INVOCATION) {
				IdlingSource.signalCanFinish();
			}
		}
	}

	private static void connectAndDiscard(
			StreamExecutionEnvironment env,
			DataStream<?> source1,
			DataStream<?> source2) {
		MultipleInputTransformation<Long> transform = new MultipleInputTransformation<>(
				"custom operator",
				new MultiplyByTwoOperatorFactory(),
				BasicTypeInfo.LONG_TYPE_INFO,
				1);

		transform.addInput(source1.getTransformation());
		transform.addInput(source2.getTransformation());

		env.addOperator(transform);
		new MultipleConnectedStreams(env)
				.transform(transform)
				.addSink(new DiscardingSink<>());
	}
}
