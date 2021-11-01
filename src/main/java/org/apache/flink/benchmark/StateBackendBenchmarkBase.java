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
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class StateBackendBenchmarkBase extends BenchmarkBase {
    public enum StateBackend {
        MEMORY,
        FS,
        FS_ASYNC,
        ROCKS,
        ROCKS_INC
    }

    public static class StateBackendContext extends FlinkEnvironmentContext {

        public final File checkpointDir;

        public final int numberOfElements = 1000;

        public DataStreamSource<IntegerLongSource.Record> source;

        public StateBackendContext() {
            try {
                checkpointDir = Files.createTempDirectory("bench-").toFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void setUp(StateBackend stateBackend, long recordsPerInvocation) throws IOException {
            try {
                super.setUp();
            } catch (Exception e) {
                e.printStackTrace();
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
                    throw new UnsupportedOperationException(
                            "Unknown state backend: " + stateBackend);
            }

            env.setStateBackend(backend);

            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            source = env.addSource(new IntegerLongSource(numberOfElements, recordsPerInvocation));
        }

        @Override
        public void tearDown() throws Exception {
            super.tearDown();
            FileUtils.deleteDirectory(checkpointDir);
        }
    }
}
