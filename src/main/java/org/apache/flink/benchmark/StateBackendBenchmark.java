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
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.FileUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(Throughput)
@Fork(value = 3, jvmArgsAppend = {
		"-Djava.rmi.server.hostname=127.0.0.1",
		"-Dcom.sun.management.jmxremote.authenticate=false",
		"-Dcom.sun.management.jmxremote.ssl=false"})
@OperationsPerInvocation(value = StateBackendBenchmark.RECORDS_PER_INVOCATION)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class StateBackendBenchmark {

	public static final int RECORDS_PER_INVOCATION = 1_000_000;

	public enum StateBackend {
		MEMORY,
		FS,
		FS_ASYNC,
		ROCKS,
		ROCKS_INC
	}

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + StateBackendBenchmark.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public void stateBackends(StateBackendContext context) throws Exception {

		context.source
				.map(new MultiplyByTwo())
				.keyBy(record -> record.key)
				.window(EventTimeSessionWindows.withGap(Time.seconds(500)))
				.reduce(new SumReduce())
				.addSink(new CollectSink());

		context.execute();
	}

	@State(Thread)
	public static class StateBackendContext {
		@Param
		public StateBackend stateBackend = StateBackend.MEMORY;

		public final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		public final File checkpointDir;

		public final int numberOfElements = 1000;

		public DataStreamSource<Record> source;

		private final int parallelism = 1;
		private final boolean objectReuse = true;

		public StateBackendContext() {
			try {
				checkpointDir = Files.createTempDirectory("bench-").toFile();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Setup
		public void setUp() throws IOException {
			// set up the execution environment
			env.setParallelism(parallelism);
			env.getConfig().disableSysoutLogging();
			if (objectReuse) {
				env.getConfig().enableObjectReuse();
			}

			final AbstractStateBackend backend;
			String checkpointDataUri = "file://" + checkpointDir.getAbsolutePath();
			switch (stateBackend) {
				case MEMORY:
					backend = new MemoryStateBackend();
					break;
				case FS:
					backend = new FsStateBackend(checkpointDataUri, false);
					break;
				case FS_ASYNC:
					backend = new FsStateBackend(checkpointDataUri, true);
					break;
				case ROCKS:
					backend = new RocksDBStateBackend(checkpointDataUri, false);
					break;
				case ROCKS_INC:
					backend = new RocksDBStateBackend(checkpointDataUri, true);
					break;
				default:
					throw new UnsupportedOperationException("Unknown state backend: " + stateBackend);
			}

			env.setStateBackend(backend);

			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			source = env.addSource(new IntegerLongSource(numberOfElements, RECORDS_PER_INVOCATION));
		}

		@TearDown
		public void tearDown() throws IOException {
			FileUtils.deleteDirectory(checkpointDir);
		}

		public void execute() throws Exception {
			env.execute();
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

		public int getKey() {
			return key;
		}

		@Override
		public String toString() {
			return String.format("(%s, %s)", key, value);
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
					ctx.collectWithTimestamp(Record.of((int) (counter % numberOfKeys), counter), counter);
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
