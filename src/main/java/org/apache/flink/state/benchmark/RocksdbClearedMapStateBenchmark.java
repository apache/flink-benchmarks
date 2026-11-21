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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.runtime.state.KeyedStateBackend;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.cleanUp;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.createKeyedStateBackend;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.getMapState;
import static org.apache.flink.state.benchmark.StateBenchmarkBase.createStateDataDir;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.MAP_KEYS;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.MAP_KEY_COUNT;

/**
 * Measures RocksDB map state reads after the maps of all keys have been cleared.
 *
 * <p>A removed map entry stays in RocksDB as a deletion marker until a compaction removes it. A
 * read of a cleared map has to skip the deletion markers of that map. It must not continue into the
 * deletion markers of the maps that follow, which this benchmark makes visible: no map has a live
 * entry that would stop such a read.
 *
 * <p>The deletion markers are read from the memtable, because the setup writes too little for
 * RocksDB to flush it. {@link RocksdbTimerServiceInitBenchmark} reads the deletion markers of
 * timers from a file.
 */
public class RocksdbClearedMapStateBenchmark extends BenchmarkBase {

    /** Few enough keys that all entries and their deletion markers stay in the memtable. */
    private static final int CLEARED_KEY_COUNT = 20_000;

    private KeyedStateBackend<Long> keyedStateBackend;

    private MapState<Long, Double> mapState;

    private List<Long> readKeys;

    private int nextKeyIndex;

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(
                                ".*"
                                        + RocksdbClearedMapStateBenchmark.class.getCanonicalName()
                                        + ".*")
                        .build();

        new Runner(opt).run();
    }

    @Setup
    public void setUp() throws Exception {
        keyedStateBackend =
                createKeyedStateBackend(
                        StateBackendBenchmarkUtils.StateBackendType.ROCKSDB, createStateDataDir());
        mapState =
                getMapState(
                        keyedStateBackend,
                        new MapStateDescriptor<>("mapState", Long.class, Double.class));
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        for (long key = 0; key < CLEARED_KEY_COUNT; key++) {
            keyedStateBackend.setCurrentKey(key);
            for (int i = 0; i < MAP_KEY_COUNT; i++) {
                mapState.put(MAP_KEYS.get(i), random.nextDouble());
            }
        }
        readKeys = new ArrayList<>(CLEARED_KEY_COUNT);
        for (long key = 0; key < CLEARED_KEY_COUNT; key++) {
            keyedStateBackend.setCurrentKey(key);
            mapState.clear();
            readKeys.add(key);
        }
        // Mix short and long unbounded seeks even when an iteration reads only a few keys.
        Collections.shuffle(readKeys, new Random(0));
        nextKeyIndex = 0;
    }

    @TearDown
    public void tearDown() throws IOException {
        cleanUp(keyedStateBackend);
    }

    @Benchmark
    public boolean clearedMapIsEmpty() throws Exception {
        keyedStateBackend.setCurrentKey(readKeys.get(nextKeyIndex));
        nextKeyIndex = (nextKeyIndex + 1) % CLEARED_KEY_COUNT;
        return mapState.isEmpty();
    }
}
