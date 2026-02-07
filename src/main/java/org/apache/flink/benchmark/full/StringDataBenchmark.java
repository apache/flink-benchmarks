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

package org.apache.flink.benchmark.full;

import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for {@link StringDataSerializer} operations on Table API StringData types.
 *
 * <p>Measures copy, serialize, and deserialize performance for compact and large BinaryStringData,
 * targeting optimizations opt-04 and opt-14.
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class StringDataBenchmark extends BenchmarkBase {

    private StringDataSerializer serializer;
    private BinaryStringData compactString;
    private BinaryStringData largeString;
    private DataOutputSerializer output;
    private DataInputDeserializer inputCompact;
    private DataInputDeserializer inputLarge;
    private byte[] serializedCompact;
    private byte[] serializedLarge;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + StringDataBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(options).run();
    }

    @Setup
    public void setup() throws IOException {
        serializer = StringDataSerializer.INSTANCE;

        // Compact string (short, fits in single segment)
        compactString = BinaryStringData.fromString("hello world");

        // Large string (1000 chars)
        StringBuilder sb = new StringBuilder(1000);
        for (int i = 0; i < 100; i++) {
            sb.append("abcdefghij");
        }
        largeString = BinaryStringData.fromString(sb.toString());

        // Pre-serialize for deserialize benchmarks
        output = new DataOutputSerializer(2048);

        serializer.serialize(compactString, output);
        serializedCompact = output.getCopyOfBuffer();

        output.clear();
        serializer.serialize(largeString, output);
        serializedLarge = output.getCopyOfBuffer();

        inputCompact = new DataInputDeserializer(serializedCompact);
        inputLarge = new DataInputDeserializer(serializedLarge);
    }

    @Benchmark
    public void copyCompactString(Blackhole bh) {
        bh.consume(serializer.copy(compactString));
    }

    @Benchmark
    public void copyLargeString(Blackhole bh) {
        bh.consume(serializer.copy(largeString));
    }

    @Benchmark
    public void serializeCompactString(Blackhole bh) throws IOException {
        output.clear();
        serializer.serialize(compactString, output);
        bh.consume(output);
    }

    @Benchmark
    public void serializeLargeString(Blackhole bh) throws IOException {
        output.clear();
        serializer.serialize(largeString, output);
        bh.consume(output);
    }

    @Benchmark
    public void deserializeCompactString(Blackhole bh) throws IOException {
        inputCompact.setBuffer(serializedCompact);
        bh.consume(serializer.deserialize(inputCompact));
    }

    @Benchmark
    public void deserializeLargeString(Blackhole bh) throws IOException {
        inputLarge.setBuffer(serializedLarge);
        bh.consume(serializer.deserialize(inputLarge));
    }
}
