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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.multipleinput.output.BlackHoleOutput;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializerV2;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetStateConfig;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.util.StringUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Objects;
import java.util.Random;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Benchmark for {@link SinkUpsertMaterializer} and {@link SinkUpsertMaterializerV2}.
 *
 * <p>It uses a test harness, not the full Flink job. All records share the same stream key. The
 * setup fills the state of that key with {@code historyLength} rows; each invocation then inserts
 * one row and retracts the row in the middle of the history, so the history length stays constant
 * during the measurement.
 *
 * <p>The benchmark has the following parameters:
 *
 * <ul>
 *   <li>strategy: LEGACY/VALUE/MAP/ADAPTIVE, as in {@code
 *       table.exec.sink.upsert-materialize-strategy.type}; LEGACY is {@link
 *       SinkUpsertMaterializer}, the others are {@link SinkUpsertMaterializerV2} with the
 *       corresponding {@link SequencedMultiSetStateConfig}
 *   <li>hasUpsertKey: true/false
 *   <li>stateBackend: HEAP/ROCKSDB
 *   <li>historyLength: number of rows kept for the stream key; longer histories make retractions
 *       more expensive
 *   <li>payloadSize: fixed to 20; a bigger payload amplifies inefficiencies, mostly on RocksDB
 * </ul>
 */
public class SinkUpsertMaterializerBenchmark extends BenchmarkBase {

    private static final int STREAM_KEY = 0;

    @Benchmark
    @OperationsPerInvocation(2)
    public void sinkUpsertMaterializer(SumBmState state) throws Exception {
        long record = state.nextRecord++;
        state.harness.processElement(insertRecord(record, STREAM_KEY, state.payload));
        state.harness.processElement(
                deleteRecord(record - state.retractOffset, STREAM_KEY, state.payload));
    }

    @State(Scope.Thread)
    public static class SumBmState {

        @Param({"LEGACY", "VALUE", "MAP", "ADAPTIVE"})
        public SumStrategy strategy;

        @Param({"false", "true"})
        public boolean hasUpsertKey;

        @Param({"HEAP", "ROCKSDB"})
        public SumStateBackend stateBackend;

        @Param({"1", "500"})
        public int historyLength;

        @Param({"20"})
        public int payloadSize;

        public String payload;

        // the rows inserted before the first retracted one are never retracted, which keeps the
        // retracted row in the middle of the history
        public long retractOffset;

        public long nextRecord;

        public KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;

        @Setup(Level.Iteration)
        public void setUp() throws Exception {
            harness = getHarness(strategy.create(hasUpsertKey), stateBackend);
            payload = StringUtils.generateRandomAlphanumericString(new Random(42), payloadSize);
            retractOffset = (historyLength + 1) / 2;
            for (nextRecord = 0; nextRecord < historyLength; nextRecord++) {
                harness.processElement(insertRecord(nextRecord, STREAM_KEY, payload));
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws Exception {
            harness.close();
            harness = null;
        }
    }

    private static final int UPSERT_KEY_POS = 0;
    private static final int STREAM_KEY_POS = 1;
    private static final int PAYLOAD_POS = 2;
    private static final LogicalType[] types =
            new LogicalType[] {new BigIntType(), new IntType(), new VarCharType()};

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> getHarness(
            OneInputStreamOperator<RowData, RowData> materializer, SumStateBackend stateBackend)
            throws Exception {
        RowDataKeySelector rowDataSelector =
                HandwrittenSelectorUtil.getRowDataSelector(new int[] {STREAM_KEY_POS}, types);
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        materializer, rowDataSelector, rowDataSelector.getProducedType());
        testHarness.getExecutionConfig().setMaxParallelism(2048);
        testHarness.setStateBackend(stateBackend.create(true));
        testHarness.setOutputCreator(ign -> new BlackHoleOutput());
        testHarness.open();
        return testHarness;
    }

    public enum SumStrategy {
        LEGACY {
            @Override
            OneInputStreamOperator<RowData, RowData> create(boolean hasUpsertKey) {
                return SinkUpsertMaterializer.create(
                        TTL_CONFIG,
                        RowType.of(types),
                        equalizer(),
                        hasUpsertKey ? upsertEqualizer() : null,
                        upsertKey(hasUpsertKey));
            }
        },
        VALUE {
            @Override
            OneInputStreamOperator<RowData, RowData> create(boolean hasUpsertKey) {
                return createV2(
                        hasUpsertKey,
                        SequencedMultiSetStateConfig.forValue(TimeDomain.EVENT_TIME, TTL_CONFIG));
            }
        },
        MAP {
            @Override
            OneInputStreamOperator<RowData, RowData> create(boolean hasUpsertKey) {
                return createV2(
                        hasUpsertKey,
                        SequencedMultiSetStateConfig.forMap(TimeDomain.EVENT_TIME, TTL_CONFIG));
            }
        },
        ADAPTIVE {
            @Override
            OneInputStreamOperator<RowData, RowData> create(boolean hasUpsertKey) {
                return createV2(
                        hasUpsertKey,
                        SequencedMultiSetStateConfig.adaptive(
                                TimeDomain.EVENT_TIME, null, null, TTL_CONFIG));
            }
        };

        // no ttl, V2 does not support it yet (FLINK-38463)
        private static final StateTtlConfig TTL_CONFIG = StateConfigUtil.createTtlConfig(0);

        abstract OneInputStreamOperator<RowData, RowData> create(boolean hasUpsertKey);

        private static OneInputStreamOperator<RowData, RowData> createV2(
                boolean hasUpsertKey, SequencedMultiSetStateConfig stateConfig) {
            return SinkUpsertMaterializerV2.create(
                    RowType.of(types),
                    equalizer(),
                    hasUpsertKey ? upsertEqualizer() : null,
                    hashFunction(),
                    hasUpsertKey ? upsertHashFunction() : null,
                    upsertKey(hasUpsertKey),
                    stateConfig);
        }

        private static int[] upsertKey(boolean hasUpsertKey) {
            return hasUpsertKey ? new int[] {UPSERT_KEY_POS} : null;
        }
    }

    private static class TestRecordEqualiser implements RecordEqualiser, HashFunction {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind()
                    && row1.getLong(UPSERT_KEY_POS) == row2.getLong(UPSERT_KEY_POS)
                    && row1.getInt(STREAM_KEY_POS) == row2.getInt(STREAM_KEY_POS)
                    && row1.getString(PAYLOAD_POS).equals(row2.getString(PAYLOAD_POS));
        }

        @Override
        public int hashCode(Object data) {
            RowData rd = (RowData) data;
            return Objects.hash(
                    rd.getLong(UPSERT_KEY_POS),
                    rd.getInt(STREAM_KEY_POS),
                    rd.getString(PAYLOAD_POS));
        }
    }

    private static class TestUpsertKeyEqualiser implements RecordEqualiser, HashFunction {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind()
                    && row1.getLong(UPSERT_KEY_POS) == row2.getLong(UPSERT_KEY_POS);
        }

        @Override
        public int hashCode(Object data) {
            return Long.hashCode(((RowData) data).getLong(UPSERT_KEY_POS));
        }
    }

    private static GeneratedRecordEqualiser equalizer() {
        return new GeneratedRecordEqualiser("", "", new Object[0]) {

            @Override
            public RecordEqualiser newInstance(ClassLoader classLoader) {
                return new TestRecordEqualiser();
            }
        };
    }

    private static GeneratedRecordEqualiser upsertEqualizer() {
        return new GeneratedRecordEqualiser("", "", new Object[0]) {

            @Override
            public RecordEqualiser newInstance(ClassLoader classLoader) {
                return new TestUpsertKeyEqualiser();
            }
        };
    }

    private static GeneratedHashFunction hashFunction() {
        return new GeneratedHashFunction("", "", new Object[0], new Configuration()) {

            @Override
            public HashFunction newInstance(ClassLoader classLoader) {
                return new TestRecordEqualiser();
            }
        };
    }

    private static GeneratedHashFunction upsertHashFunction() {
        return new GeneratedHashFunction("", "", new Object[0], new Configuration()) {

            @Override
            public HashFunction newInstance(ClassLoader classLoader) {
                return new TestUpsertKeyEqualiser();
            }
        };
    }

    public enum SumStateBackend {
        HEAP {

            public StateBackend create(boolean incrementalIfSupported) {
                return new HashMapStateBackend();
            }
        },
        ROCKSDB {

            public StateBackend create(boolean incrementalIfSupported) {
                return new EmbeddedRocksDBStateBackend(incrementalIfSupported);
            }
        };

        public abstract StateBackend create(boolean incrementalIfSupported);
    }
}
