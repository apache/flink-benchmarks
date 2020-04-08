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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.benchmark.SerializationFrameworkMiniBenchmarks;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.annotations.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PojoSerializationBenchmark extends BenchmarkBase {

    SerializationFrameworkMiniBenchmarks.MyPojo pojo;
    org.apache.flink.benchmark.avro.MyPojo avroPojo;

    ExecutionConfig config = new ExecutionConfig();
    TypeSerializer<SerializationFrameworkMiniBenchmarks.MyPojo> pojoSerializer =
            TypeInformation.of(SerializationFrameworkMiniBenchmarks.MyPojo.class).createSerializer(config);
    TypeSerializer<SerializationFrameworkMiniBenchmarks.MyPojo> kryoSerializer =
            new KryoSerializer<>(SerializationFrameworkMiniBenchmarks.MyPojo.class, config);
    TypeSerializer<org.apache.flink.benchmark.avro.MyPojo> avroSerializer =
            new AvroSerializer<>(org.apache.flink.benchmark.avro.MyPojo.class);

    ByteArrayInputStream pojoBuffer;
    ByteArrayInputStream avroBuffer;
    ByteArrayInputStream kryoBuffer;


    @Setup
    public void setup() throws IOException {
        pojo = new SerializationFrameworkMiniBenchmarks.MyPojo(
                0,
                "myName",
                new String[] {"op1", "op2", "op3", "op4"},
                new SerializationFrameworkMiniBenchmarks.MyOperation[] {
                        new SerializationFrameworkMiniBenchmarks.MyOperation(1, "op1"),
                        new SerializationFrameworkMiniBenchmarks.MyOperation(2, "op2"),
                        new SerializationFrameworkMiniBenchmarks.MyOperation(3, "op3")},
                1,
                2,
                3,
                "null");
        avroPojo = new org.apache.flink.benchmark.avro.MyPojo(
                0,
                "myName",
                Arrays.asList("op1", "op2", "op3", "op4"),
                Arrays.asList(
                        new org.apache.flink.benchmark.avro.MyOperation(1, "op1"),
                        new org.apache.flink.benchmark.avro.MyOperation(2, "op2"),
                        new org.apache.flink.benchmark.avro.MyOperation(3, "op3")),
                1,
                2,
                3,
                "null");
        pojoBuffer = new ByteArrayInputStream(write(pojoSerializer, pojo));
        avroBuffer = new ByteArrayInputStream(write(avroSerializer, avroPojo));
        kryoBuffer = new ByteArrayInputStream(write(kryoSerializer, pojo));
    }

    public static void main(String[] args)
            throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + PojoSerializationBenchmark.class.getCanonicalName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Benchmark
    public byte[] writePojo() throws IOException {
        return write(pojoSerializer, pojo);
    }

    @Benchmark
    public byte[] writeAvro() throws IOException {
        return write(avroSerializer, avroPojo);
    }

    @Benchmark
    public byte[] writeKryo() throws IOException {
        return write(kryoSerializer, pojo);
    }

    @Benchmark
    public SerializationFrameworkMiniBenchmarks.MyPojo readPojo() throws IOException {
        pojoBuffer.reset();
        return pojoSerializer.deserialize(new DataInputViewStreamWrapper(pojoBuffer));
    }

    @Benchmark
    public SerializationFrameworkMiniBenchmarks.MyPojo readKryo() throws IOException {
        kryoBuffer.reset();
        return kryoSerializer.deserialize(new DataInputViewStreamWrapper(kryoBuffer));
    }

    @Benchmark
    public org.apache.flink.benchmark.avro.MyPojo readAvro() throws IOException {
        avroBuffer.reset();
        return avroSerializer.deserialize(new DataInputViewStreamWrapper(avroBuffer));
    }

    private <T> byte[] write(TypeSerializer<T> serializer, T value) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputView out = new DataOutputViewStreamWrapper(buffer);
        serializer.serialize(value, out);
        return buffer.toByteArray();
    }
}
