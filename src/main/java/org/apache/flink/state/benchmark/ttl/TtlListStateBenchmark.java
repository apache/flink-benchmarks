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

package org.apache.flink.state.benchmark.ttl;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.state.benchmark.StateBenchmarkBase;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.applyToAllKeys;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.compactState;
import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.getListState;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.listValueCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.setupKeyCount;

/** Implementation for list state benchmark testing. */
public class TtlListStateBenchmark extends TtlStateBenchmarkBase {
    private final String STATE_NAME = "listState";
    private ListStateDescriptor<Long> stateDesc;
    private ListState<Long> listState;
    private List<Long> dummyLists;

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + TtlListStateBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }

    @Setup
    public void setUp() throws Exception {
        keyedStateBackend = createKeyedStateBackend();
        stateDesc = configTtl(new ListStateDescriptor<>(STATE_NAME, Long.class));
        listState = getListState(keyedStateBackend, stateDesc);
        dummyLists = new ArrayList<>(listValueCount);
        for (int i = 0; i < listValueCount; ++i) {
            dummyLists.add(random.nextLong());
        }
        keyIndex = new AtomicInteger();
    }

    @Setup(Level.Iteration)
    public void setUpPerIteration() throws Exception {
        for (int i = 0; i < setupKeyCount; ++i) {
            keyedStateBackend.setCurrentKey((long) i);
            setTtlWhenInitialization();
            listState.add(random.nextLong());
        }
        // make sure only one sst file left, so all get invocation will access this single file,
        // to prevent the spike caused by different key distribution in multiple sst files,
        // the more access to the older sst file, the lower throughput will be.
        compactState(keyedStateBackend, stateDesc);
        advanceTimePerIteration();
    }

    @TearDown(Level.Iteration)
    public void tearDownPerIteration() throws Exception {
        applyToAllKeys(
                keyedStateBackend,
                stateDesc,
                (k, state) -> {
                    keyedStateBackend.setCurrentKey(k);
                    state.clear();
                });
        // make the clearance effective, trigger compaction for RocksDB, and GC for heap.
        if (!compactState(keyedStateBackend, stateDesc)) {
            System.gc();
        }
        // wait a while for the clearance to take effect.
        Thread.sleep(1000);
    }

    @Benchmark
    public void listUpdate(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        listState.update(keyValue.listValue);
    }

    @Benchmark
    public void listAdd(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.newKey);
        listState.update(keyValue.listValue);
    }

    @Benchmark
    public void listAppend(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        listState.add(keyValue.value);
    }

    @Benchmark
    public Iterable<Long> listGet(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        return listState.get();
    }

    @Benchmark
    public void listGetAndIterate(StateBenchmarkBase.KeyValue keyValue, Blackhole bh) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        Iterable<Long> iterable = listState.get();
        for (Long value : iterable) {
            bh.consume(value);
        }
    }

    @Benchmark
    public void listAddAll(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        listState.addAll(dummyLists);
    }
}
