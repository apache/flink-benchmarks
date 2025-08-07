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
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.multipleinput.output.BlackHoleOutput;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Benchmark for {@link SinkUpsertMaterializer}.
 * <p>
 * It inserts a fixed number of records (inserts and deletes) in a loop and measures the time it takes to handle all the records. It uses a test harness, not the full Flink job.
 * <p>
 The benchmark has the following parameters:
 <ul>

 <li>total number of records - fixed to 10K; all records are inserted under the same stream key</li>
 <li>hasUpsertKey: true/false</li>
 <li>stateBackend type: HEAP/RocksDB</li>
 <li>payload size - fixed to 100 bytes; bigger payload degrades faster on RocksDB</li>
 <li>retractPercentage, retractDelay - how many records to retract, and from which record to start retracting; both parameters control how frequently retraction happens and how long the history is.
 Retraction is performed from the middle of the list. Retraction is inefficient in the current implementation on long lists.
 </ul>

 * Results on M3Pro:
 *
 * <pre>{@code
Benchmark                            (hasUpsertKey)  (payloadSize)  (retractDelay)  (retractPercentage)  (stateBackend)  (sumVersion)   Mode  Cnt     Score   Error   Units
SinkUpsertMaterializerBenchmark.run           false            100               1                  100            HEAP            V1  thrpt       4138.808          ops/ms
SinkUpsertMaterializerBenchmark.run           false            100               1                  100         ROCKSDB            V1  thrpt        284.055          ops/ms
SinkUpsertMaterializerBenchmark.run           false            100              10                  100            HEAP            V1  thrpt       3729.824          ops/ms
SinkUpsertMaterializerBenchmark.run           false            100              10                  100         ROCKSDB            V1  thrpt        205.047          ops/ms
SinkUpsertMaterializerBenchmark.run           false            100             100                  100            HEAP            V1  thrpt       4137.591          ops/ms
SinkUpsertMaterializerBenchmark.run           false            100             100                  100         ROCKSDB            V1  thrpt         80.406          ops/ms
SinkUpsertMaterializerBenchmark.run           false            100             200                  100            HEAP            V1  thrpt       1886.574          ops/ms
SinkUpsertMaterializerBenchmark.run           false            100             200                  100         ROCKSDB            V1  thrpt         30.935          ops/ms
SinkUpsertMaterializerBenchmark.run           false            100            1000                  100            HEAP            V1  thrpt        546.826          ops/ms
SinkUpsertMaterializerBenchmark.run           false            100            1000                  100         ROCKSDB            V1  thrpt          7.081          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100               1                  100            HEAP            V1  thrpt       4006.263          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100               1                  100         ROCKSDB            V1  thrpt        297.556          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100              10                  100            HEAP            V1  thrpt       3240.089          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100              10                  100         ROCKSDB            V1  thrpt        209.375          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100             100                  100            HEAP            V1  thrpt       2131.445          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100             100                  100         ROCKSDB            V1  thrpt         78.209          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100             200                  100            HEAP            V1  thrpt        652.936          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100             200                  100         ROCKSDB            V1  thrpt         29.674          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100            1000                  100            HEAP            V1  thrpt        118.567          ops/ms
SinkUpsertMaterializerBenchmark.run            true            100            1000                  100         ROCKSDB            V1  thrpt          6.426          ops/ms
 * }</pre>
 */
@OperationsPerInvocation(value = SinkUpsertMaterializerBenchmark.RECORDS_PER_INVOCATION)
@SuppressWarnings("ConstantValue")
public class SinkUpsertMaterializerBenchmark extends BenchmarkBase {

    private static final int STREAM_KEY = 0;

    public static void main(String[] args) throws RunnerException {
        new Runner(new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                // speedup
//                .warmupIterations(1)
//                .measurementIterations(1)
//                .forks(1)
//                .warmupTime(TimeValue.milliseconds(100))
//                .measurementTime(TimeValue.seconds(1))
                .include(".*" + SinkUpsertMaterializerBenchmark.class.getCanonicalName() + ".*")
                .build()).run();
    }

    @Benchmark
    public void run(SumBmState state) throws Exception {
        for (long record = 0; record < state.numRecordsTotal; record++) {
            state.harness.processElement(insertRecord(record, STREAM_KEY, state.payload));
            if (state.shouldRetract(record)) {
                state.harness.processElement(
                        deleteRecord(record - state.retractOffset, STREAM_KEY, state.payload));
            }
        }
    }

    protected static final int RECORDS_PER_INVOCATION = 10_000;

    @State(Scope.Thread)
    public static class SumBmState {

        @Param({"false", "true"}) public boolean hasUpsertKey;

        @Param({"HEAP", "ROCKSDB"}) public SumStateBackend stateBackend;

        public int numRecordsTotal;

        // larger payload amplifies any inefficiencies but slows down the benchmark; mostly affects rocksdb
        @Param({"10", "250"})
        public int payloadSize;

        // lower retraction percentage implies longer history, making retractions even harder (unless percentage = 0)
        @Param("100")
        public int retractPercentage;

        // higher retraction delay leaves longer history, making retractions even harder (unless percentage = 0)
        // for automated runs, reduce the run time (and the data points) to the most common cases
        @Param({"1", "1000"})
        // for comparison, the following values might be useful:
        // @Param({"1", "10", "100", "200", "1000"})
        public int retractDelay;

        // the lower the value, the closer to the end of the list is the element to retract, the harder for V1 to find the element
        public long retractOffset;

        public String payload;

        public KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness;

        @Setup(Level.Invocation)
        public void initSumBmState() throws Exception {
            harness = getHarness(createSum(hasUpsertKey), stateBackend);
            payload = generatePayload(payloadSize);
            numRecordsTotal = RECORDS_PER_INVOCATION;
            retractOffset = (1 + retractDelay) / 2;
            checkState(numRecordsTotal > retractDelay);
            checkState(retractPercentage >= 0 && retractPercentage <= 100);
        }


        @TearDown(Level.Invocation)
        public void teardown() throws Exception {
            this.harness.close();
            this.harness = null;
        }

        public boolean shouldRetract(long record) {
            return retractEnabled() && (retractEverything() || retractRecord(record));
        }

        private boolean retractEnabled() {
            return retractPercentage > 0;
        }

        private boolean retractEverything() {
            return retractPercentage == 100;
        }

        private boolean retractRecord(long index) {
            return index >= retractDelay && index % 100 < retractPercentage;
        }
    }

    private static String generatePayload(int size) {
        final byte[] bytes = new byte[size];
        new Random().nextBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static final int UPSERT_KEY_POS = 0;
    private static final int STREAM_KEY_POS = 1;
    private static final int PAYLOAD_POS = 2;
    private static final LogicalType[] types =
            new LogicalType[] {new BigIntType(), new IntType(), new VarCharType()};

    private static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> getHarness(
            OneInputStreamOperator<RowData, RowData> materializer, SumStateBackend stateBackend) throws Exception {
        RowDataKeySelector rowDataSelector =
                HandwrittenSelectorUtil.getRowDataSelector(new int[] {STREAM_KEY_POS}, types);
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                materializer, rowDataSelector, rowDataSelector.getProducedType());
        testHarness.getExecutionConfig().setMaxParallelism(2048);
        testHarness.setStateBackend(stateBackend.create(true));
        testHarness.setOutputCreator(ign -> new BlackHoleOutput()); // requires change in Flink
        testHarness.open();
        return testHarness;
    }

    private static OneInputStreamOperator<RowData, RowData> createSum(boolean hasUpsertKey) {
        StateTtlConfig ttlConfig = StateConfigUtil.createTtlConfig(0); // no ttl
        RowType physicalRowType = RowType.of(types);
        int[] inputUpsertKey = hasUpsertKey ? new int[] {UPSERT_KEY_POS} : null;
        return new SinkUpsertMaterializer(
                ttlConfig,
                InternalSerializers.create(physicalRowType),
                equalizer(),
                hasUpsertKey ? upsertEqualizer() : null,
                inputUpsertKey);
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
                    rd.getLong(UPSERT_KEY_POS), rd.getInt(STREAM_KEY_POS), rd.getString(PAYLOAD_POS));
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
