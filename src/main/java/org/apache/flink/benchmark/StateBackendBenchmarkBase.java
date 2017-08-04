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

import org.apache.flink.benchmark.functions.IntegerLongSource;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

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
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class StateBackendBenchmarkBase {
	public enum StateBackend {
		MEMORY,
		FS,
		FS_ASYNC,
		ROCKS,
		ROCKS_INC
	}

	public static class StateBackendContext {
		public final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		public final File checkpointDir;

		public final int numberOfElements = 1000;

		public DataStreamSource<IntegerLongSource.Record> source;

		private final int parallelism = 1;
		private final boolean objectReuse = true;

		public StateBackendContext() {
			try {
				checkpointDir = Files.createTempDirectory("bench-").toFile();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public void setUp(StateBackend stateBackend, long recordsPerInvocation) throws IOException {
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
			source = env.addSource(new IntegerLongSource(numberOfElements, recordsPerInvocation));
		}

		public void tearDown() throws IOException {
			FileUtils.deleteDirectory(checkpointDir);
		}

		public void execute() throws Exception {
			env.execute();
		}
	}
}
