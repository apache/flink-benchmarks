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
package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;


import java.io.File;
import java.io.IOException;
import java.util.Random;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
public class RocksIncrementalCheckpointReScaleBenchmark extends BenchmarkBase {

    @Param({"3"})
    int parallelism1;  // before rescaling

    @Param({"4"})
    int parallelism2;  // after rescaling

    @Param({"0", "1", "2", "3"})
    int subtaskIndex;

    @Param({"heap", "fileSystem", "rocksdb", "rocksdbIncr"})
    String backendType;

    private File rootDir;

    private OperatorSubtaskState initSnapshot;

    private int maxParallelism = 10;

    // length of key
    private int wordLen = 32;

    // number of keys send by source, wordLen and numberElements are used to control the size of state together
    private int numberElements = 50000;

    public static void main(String[] args)
            throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + RocksIncrementalCheckpointReScaleBenchmark.class.getCanonicalName() + ".*")
                .build();

        new Runner(options).run();

    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        char[] fatArray = new char[wordLen];
        Random random = new Random(0);
        for (int i = 0; i < fatArray.length; i++) {
            fatArray[i] = (char) random.nextInt();
        }

        KeySelector<String, String> keySelector = new TestKeySelector();
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<String, String, Integer> harness =
                     getHarnessTest(keySelector, maxParallelism, 1, 0)) {
            harness.setStateBackend(getStateBackend(backendType));
            harness.open();
            for (int i = 0; i < numberElements; i++) {
                harness.processElement(new StreamRecord<>(covertToString(i, fatArray), 0));
            }
            snapshot = harness.snapshot(0, 1);
        }


        KeyedOneInputStreamOperatorTestHarness<String, String, Integer>[] initHarness =
                new KeyedOneInputStreamOperatorTestHarness[parallelism1];
        OperatorSubtaskState[] initHandles = new OperatorSubtaskState[parallelism1];

        try {
            for (int i = 0; i < parallelism1; i++) {
                OperatorSubtaskState subtaskState =
                        AbstractStreamOperatorTestHarness.repartitionOperatorState(
                                snapshot, maxParallelism, 1, parallelism1, i);

                initHarness[i] =
                        getHarnessTest(keySelector, maxParallelism, parallelism1, i);
                initHarness[i].setStateBackend(getStateBackend(backendType));
                initHarness[i].setup();
                initHarness[i].initializeState(subtaskState);
                initHarness[i].open();
                initHandles[i] = initHarness[i].snapshot(1, 2);
                subtaskState.discardState();
            }
            initSnapshot = AbstractStreamOperatorTestHarness.repackageState(initHandles);
            System.out.println(initSnapshot.getStateSize());
        } finally {
            snapshot.discardState();
            closeHarness(initHarness);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        initSnapshot.discardState();
        if (rootDir != null && rootDir.exists()) {
            rootDir.delete();
        }
    }

    @Benchmark
    public void rescale() throws Exception {
        OperatorSubtaskState subtaskState =
                AbstractStreamOperatorTestHarness.repartitionOperatorState(
                        initSnapshot, maxParallelism, parallelism1, parallelism2, subtaskIndex);
        KeyedOneInputStreamOperatorTestHarness<String, String, Integer> harness = getHarnessTest(new TestKeySelector(), maxParallelism, parallelism2, subtaskIndex);
        harness.setStateBackend(getStateBackend(backendType));
        harness.setup();
        harness.initializeState(subtaskState);
        subtaskState.discardState();
        harness.close();
    }


    private void closeHarness(KeyedOneInputStreamOperatorTestHarness<?, ?, ?>[] harnessArr)
            throws Exception {
        for (KeyedOneInputStreamOperatorTestHarness<?, ?, ?> harness : harnessArr) {
            if (harness != null) {
                harness.close();
            }
        }
    }

    public StateBackend getStateBackend(String stateBackendType) throws IOException {
        switch (stateBackendType) {
            case "heap":
                return new HashMapStateBackend();
            case "fileSystem":
                rootDir = prepareDirectory("benchmark");
                return new FsStateBackend("file://" + rootDir.getAbsolutePath());
            case "rocksdbIncr":
                return new EmbeddedRocksDBStateBackend(true);
            case "rocksdb":
                return new EmbeddedRocksDBStateBackend(false);
            default:
                throw new IllegalArgumentException("Unknown state backend type: " + stateBackendType);
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<String, String, Integer> getHarnessTest(
            KeySelector<String, String> keySelector,
            int maxParallelism,
            int taskParallelism,
            int subtaskIdx)
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new TestKeyedFunction()),
                keySelector,
                BasicTypeInfo.STRING_TYPE_INFO,
                maxParallelism,
                taskParallelism,
                subtaskIdx);
    }

    /**
     * word count.
     */
    private class TestKeyedFunction extends KeyedProcessFunction<String, String, Integer> {

        private ValueState<Integer> counterState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            counterState =
                    this.getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("counter", Integer.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<Integer> out)
                throws Exception {
            Integer oldCount = counterState.value();
            Integer newCount = oldCount != null ? oldCount + 1 : 1;
            counterState.update(newCount);
            out.collect(newCount);
        }
    }

    private class TestKeySelector implements KeySelector<String, String> {
        @Override
        public String getKey(String value) throws Exception {
            return value;
        }
    }

    private String covertToString(int number, char[] fatArray) {
        String a = String.valueOf(number);
        StringBuilder builder = new StringBuilder(wordLen);
        builder.append(a);
        builder.append(fatArray, 0, wordLen - a.length());
        return builder.toString();
    }

    private static File prepareDirectory(String prefix) throws IOException {
        File target = File.createTempFile(prefix, "");
        if (target.exists() && !target.delete()) {
            throw new IOException("Target dir {" + target.getAbsolutePath() + "} exists but failed to clean it up");
        } else if (!target.mkdirs()) {
            throw new IOException("Failed to create target directory: " + target.getAbsolutePath());
        } else {
            return target;
        }
    }
}