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
import org.apache.flink.benchmark.functions.IntegerLongSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

@OperationsPerInvocation(value = WindowBenchmarks.RECORDS_PER_INVOCATION)
public class WindowBenchmarks extends BenchmarkBase {

	public static final int RECORDS_PER_INVOCATION = 7_000_000;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + WindowBenchmarks.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public void globalWindow(TimeWindowContext context) throws Exception {
		IntLongApplications.reduceWithWindow(context.source, GlobalWindows.create());
		context.execute();
	}

	@Benchmark
	public void tumblingWindow(TimeWindowContext context) throws Exception {
		IntLongApplications.reduceWithWindow(context.source, TumblingEventTimeWindows.of(Time.seconds(10_000)));
		context.execute();
	}

	@Benchmark
	public void slidingWindow(TimeWindowContext context) throws Exception {
		IntLongApplications.reduceWithWindow(context.source, SlidingEventTimeWindows.of(Time.seconds(10_000), Time.seconds(1000)));
		context.execute();
	}

	@Benchmark
	public void sessionWindow(TimeWindowContext context) throws Exception {
		IntLongApplications.reduceWithWindow(context.source, EventTimeSessionWindows.withGap(Time.seconds(500)));
		context.execute();
	}

	public static class TimeWindowContext extends Context {
		public final int numberOfElements = 1000;

		public DataStreamSource<IntegerLongSource.Record> source;

		@Setup
		@Override
		public void setUp() throws IOException {
			super.setUp();

			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			source = env.addSource(new IntegerLongSource(numberOfElements, RECORDS_PER_INVOCATION));
		}
	}
}
