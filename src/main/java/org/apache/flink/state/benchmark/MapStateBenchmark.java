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
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapKeyCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapKeys;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.setupKeyCount;

/**
 * Implementation for map state benchmark testing.
 */
public class MapStateBenchmark extends StateBenchmarkBase {
    private MapState<Long, Double> mapState;
    private Map<Long, Double> dummyMaps;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + MapStateBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setUp() throws Exception {
        keyedStateBackend = createKeyedStateBackend();
        mapState = keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new MapStateDescriptor<>("mapState", Long.class, Double.class));
        dummyMaps = new HashMap<>(mapKeyCount);
        for (int i = 0; i < mapKeyCount; ++i) {
            dummyMaps.put(mapKeys.get(i), random.nextDouble());
        }
        for (int i = 0; i < setupKeyCount; ++i) {
            keyedStateBackend.setCurrentKey((long) i);
            for (int j = 0; j < mapKeyCount; j++) {
                mapState.put(mapKeys.get(j), random.nextDouble());
            }
        }
        keyIndex = new AtomicInteger();
    }

    @Benchmark
    public void mapUpdate(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        mapState.put(keyValue.mapKey, keyValue.mapValue);
    }

    @Benchmark
    public void mapAdd(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.newKey);
        mapState.put(keyValue.mapKey, keyValue.mapValue);
    }

    @Benchmark
    public Double mapGet(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        return mapState.get(keyValue.mapKey);
    }

    @Benchmark
    public boolean mapContains(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        return mapState.contains(keyValue.mapKey << 1);
    }

    @Benchmark
    public boolean mapIsEmpty(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        return mapState.isEmpty();
    }

    @Benchmark
    @OperationsPerInvocation(mapKeyCount)
    public void mapKeys(KeyValue keyValue, Blackhole bh) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        for (Long key : mapState.keys()) {
            bh.consume(key);
        }
    }

    @Benchmark
    @OperationsPerInvocation(mapKeyCount)
    public void mapValues(KeyValue keyValue, Blackhole bh) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        for (Double value : mapState.values()) {
            bh.consume(value);
        }
    }

    @Benchmark
    @OperationsPerInvocation(mapKeyCount)
    public void mapEntries(KeyValue keyValue, Blackhole bh) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        Iterable<Map.Entry<Long, Double>> iterable = mapState.entries();
        if (iterable != null) {
            for (Map.Entry<Long, Double> entry : mapState.entries()) {
                bh.consume(entry.getKey());
                bh.consume(entry.getValue());
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(mapKeyCount)
    public void mapIterator(KeyValue keyValue, Blackhole bh) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        Iterator<Map.Entry<Long, Double>> iterator = mapState.iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Double> entry = iterator.next();
            bh.consume(entry.getKey());
            bh.consume(entry.getValue());
        }
    }

    @Benchmark
    public void mapRemove(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        mapState.remove(keyValue.mapKey);
    }

    @Benchmark
    public void mapPutAll(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        mapState.putAll(dummyMaps);
    }
}
