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
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.config.ConfigUtil;
import org.apache.flink.config.StateBenchmarkOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.benchmark.RescalingBenchmark;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import static org.apache.flink.state.benchmark.StateBenchmarkBase.createStateDataDir;

public class RescalingBenchmarkBase extends BenchmarkBase {

    @Param({"RESCALE_IN", "RESCALE_OUT"})
    protected RescaleType rescaleType;

    protected RescalingBenchmark<byte[]> benchmark;

    public static void runBenchmark(Class<?> clazz) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + clazz.getCanonicalName() + ".*")
                        .build();

        new Runner(options).run();
    }

    protected static File prepareDirectory(String prefix) throws IOException {
        return StateBackendBenchmarkUtils.prepareDirectory(prefix, createStateDataDir());
    }

    @State(Scope.Thread)
    public enum RescaleType {
        RESCALE_OUT(1, 2, 0),
        RESCALE_IN(2, 1, 0);

        private final int parallelismBefore;
        private final int parallelismAfter;
        private final int subtaskIndex;

        RescaleType(int parallelismBefore, int parallelismAfter, int subtaskIdx) {
            this.parallelismBefore = parallelismBefore;
            this.parallelismAfter = parallelismAfter;
            this.subtaskIndex = subtaskIdx;
        }

        public int getParallelismBefore() {
            return parallelismBefore;
        }

        public int getParallelismAfter() {
            return parallelismAfter;
        }

        public int getSubtaskIndex() {
            return subtaskIndex;
        }
    }

    protected static class ByteArrayRecordGenerator
            implements RescalingBenchmark.StreamRecordGenerator<byte[]> {
        private final Random random = new Random(0);
        private final int numberOfKeys;
        private final byte[] fatArray;
        private int count = 0;


        protected ByteArrayRecordGenerator(final int numberOfKeys,
                                           final int keyLen) {
            this.numberOfKeys = numberOfKeys;
            fatArray = new byte[keyLen];
        }

        // generate deterministic elements for source
        @Override
        public Iterator<StreamRecord<byte[]>> generate() {
            return new Iterator<StreamRecord<byte[]>>() {
                @Override
                public boolean hasNext() {
                    return count < numberOfKeys;
                }

                @Override
                public StreamRecord<byte[]> next() {
                    random.nextBytes(fatArray);
                    changePrefixOfArray(count, fatArray);
                    // make the hashcode of keys different.
                    StreamRecord<byte[]> record =
                            new StreamRecord<>(Arrays.copyOf(fatArray, fatArray.length), 0);
                    count += 1;
                    return record;
                }
            };
        }

        @Override
        public TypeInformation getTypeInformation() {
            return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
        }

        private void changePrefixOfArray(int number, byte[] fatArray) {
            fatArray[0] = (byte) ((number >> 24) & 0xFF);
            fatArray[1] = (byte) ((number >> 16) & 0xFF);
            fatArray[2] = (byte) ((number >> 8) & 0xFF);
            fatArray[3] = (byte) (number & 0xFF);
        }
    }

    protected static class TestKeyedFunction extends KeyedProcessFunction<byte[], byte[], Void> {

        private static final long serialVersionUID = 1L;
        private final Random random = new Random(0);
        private final int valueLen = 128;

        private ValueState<byte[]> randomState;
        private final byte[] stateArray = new byte[valueLen];

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            randomState =
                    this.getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("RandomState", byte[].class));
        }

        @Override
        public void processElement(
                byte[] value,
                KeyedProcessFunction<byte[], byte[], Void>.Context ctx,
                Collector<Void> out)
                throws Exception {
            random.nextBytes(stateArray);
            randomState.update(Arrays.copyOf(stateArray, stateArray.length));
        }
    }
}
