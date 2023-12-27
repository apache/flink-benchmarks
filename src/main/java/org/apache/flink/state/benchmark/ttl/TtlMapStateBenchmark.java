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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.state.benchmark.StateBenchmarkBase;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
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

import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.getMapState;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapKeyCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapKeys;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.setupKeyCount;

/** Implementation for map state benchmark testing. */
public class TtlMapStateBenchmark extends TtlStateBenchmarkBase {
    private MapState<Long, Double> mapState;
    private Map<Long, Double> dummyMaps;

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + TtlMapStateBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(opt).run();
    }

    @Setup
    public void setUp() throws Exception {
        keyedStateBackend = createKeyedStateBackend();
        mapState =
                getMapState(
                        keyedStateBackend,
                        configTtl(new MapStateDescriptor<>("mapState", Long.class, Double.class)));
        dummyMaps = new HashMap<>(mapKeyCount);
        for (int i = 0; i < mapKeyCount; ++i) {
            dummyMaps.put(mapKeys.get(i), random.nextDouble());
        }
        for (int i = 0; i < setupKeyCount; ++i) {
            keyedStateBackend.setCurrentKey((long) i);
            for (int j = 0; j < mapKeyCount; j++) {
                setTtlWhenInitialization();
                mapState.put(mapKeys.get(j), random.nextDouble());
            }
        }
        keyIndex = new AtomicInteger();
    }

    @Setup(Level.Iteration)
    public void setUpPerIteration() throws Exception {
        advanceTimePerIteration();
    }

    @Benchmark
    public void mapUpdate(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        mapState.put(keyValue.mapKey, keyValue.mapValue);
    }

    @Benchmark
    public void mapAdd(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.newKey);
        mapState.put(keyValue.mapKey, keyValue.mapValue);
    }

    @Benchmark
    public Double mapGet(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        return mapState.get(keyValue.mapKey);
    }

    @Benchmark
    public boolean mapIsEmpty(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        return mapState.isEmpty();
    }

    @Benchmark
    @OperationsPerInvocation(mapKeyCount)
    public void mapIterator(StateBenchmarkBase.KeyValue keyValue, Blackhole bh) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        Iterator<Map.Entry<Long, Double>> iterator = mapState.iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Double> entry = iterator.next();
            bh.consume(entry.getKey());
            bh.consume(entry.getValue());
        }
    }

    @Benchmark
    public void mapPutAll(StateBenchmarkBase.KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        mapState.putAll(dummyMaps);
    }
}
