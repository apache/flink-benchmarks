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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

public class KeyByBenchmark extends BenchmarkBase {

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + KeyByBenchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public void globalWindow(KeyByContext context) throws Exception {

		context.source
				.map(new MultiplyByTwo())
				.keyBy(record -> record.key)
				.window(GlobalWindows.create())
				.reduce(new SumReduce())
				.addSink(new CollectSink<>());

		context.execute();
	}

	public static class KeyByContext extends Context {
		@Param({"10", "1000"})
		public int numberOfElements;

		public DataStreamSource<Record> source;

		@Setup
		@Override
		public void setUp() throws IOException {
			super.setUp();

			source = env.addSource(new IntegerLongSource(numberOfElements, RECORDS_PER_INVOCATION));
		}
	}

	private static class MultiplyByTwo implements MapFunction<Record, Record> {
		@Override
		public Record map(Record value) throws Exception {
			return Record.of(value.key, value.value * 2);
		}
	}

	private static class SumReduce implements ReduceFunction<Record> {
		@Override
		public Record reduce(Record var1, Record var2) throws Exception {
			return Record.of(var1.key, var1.value + var2.value);
		}
	}

	public static class Record {

		public final int key;
		public final long value;

		public Record() {
			this(0, 0);
		}

		public Record(int key, long value) {
			this.key = key;
			this.value = value;
		}

		public static Record of(int key, long value) {
			return new Record(key, value);
		}
	}

	public static class IntegerLongSource extends RichParallelSourceFunction<Record> {

		private volatile boolean running = true;
		private int numberOfKeys;
		private long numberOfElements;

		public IntegerLongSource(int numberOfKeys, long numberOfElements) {
			this.numberOfKeys = numberOfKeys;
			this.numberOfElements = numberOfElements;
		}

		@Override
		public void run(SourceContext<Record> ctx) throws Exception {
			long counter = 0;

			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(Record.of((int) (counter % numberOfKeys), counter));
					counter++;
					if (counter >= numberOfElements) {
						cancel();
					}
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
