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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

/**
 * Benchmark for keyBy() on tuples and arrays.
 */
public class KeyByBenchmarks extends BenchmarkBase {

	private static final int TUPLE_RECORDS_PER_INVOCATION = 15_000_000;
	private static final int ARRAY_RECORDS_PER_INVOCATION = 7_000_000;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + KeyByBenchmarks.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	@OperationsPerInvocation(value = KeyByBenchmarks.TUPLE_RECORDS_PER_INVOCATION)
	public void tupleKeyBy() throws Exception {
		LocalStreamEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironment(4);

		env.addSource(new IncreasingTupleSource(TUPLE_RECORDS_PER_INVOCATION, 10))
				.keyBy(0)
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = KeyByBenchmarks.ARRAY_RECORDS_PER_INVOCATION)
	public void arrayKeyBy() throws Exception {
		LocalStreamEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironment(4);

		env.addSource(new IncreasingArraySource(ARRAY_RECORDS_PER_INVOCATION, 10))
				.keyBy(0)
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	private abstract static class IncreasingBaseSource<T> implements ParallelSourceFunction<T> {
		private static final long serialVersionUID = 8318018060123048234L;

		final int numKeys;
		int remainingEvents;

		IncreasingBaseSource(int numEvents, int numKeys) {
			this.remainingEvents = numEvents;
			this.numKeys = numKeys;
		}

		@Override
		public void cancel() {
			this.remainingEvents = 0;
		}
	}

	private static class IncreasingTupleSource extends IncreasingBaseSource<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 2941333602938145526L;

		IncreasingTupleSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> out) {
			int keyId = 0;
			while (--remainingEvents >= 0) {
				synchronized (out.getCheckpointLock()) {
					out.collect(new Tuple2<>(keyId++, 1));
				}
				if (keyId >= numKeys) {
					keyId = 0;
				}
			}
		}

	}

	private static class IncreasingArraySource extends IncreasingBaseSource<int[]> {
		private static final long serialVersionUID = -7883758559005221998L;

		IncreasingArraySource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		public void run(SourceContext<int[]> out) {
			int keyId = 0;
			while (--remainingEvents >= 0) {
				synchronized (out.getCheckpointLock()) {
					out.collect(new int[] {keyId++, 1});
				}
				if (keyId >= numKeys) {
					keyId = 0;
				}
			}
		}
	}
}
