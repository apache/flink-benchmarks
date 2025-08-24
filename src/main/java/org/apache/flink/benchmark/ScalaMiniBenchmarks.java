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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

/**
 * Benchmark for serializing Scala Case Classes.
 */
public class ScalaMiniBenchmarks extends BenchmarkBase {

    protected static final int RECORDS_PER_INVOCATION = 300_000;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(String.format(".*%s.*", ScalaMiniBenchmarks.class.getCanonicalName()))
                        .build();
        new Runner(options).run();
    }

    @Benchmark
    @OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
    public void serializerScala(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(4);

        env.addSource(new ScalaSource(RECORDS_PER_INVOCATION, 10), ScalaCaseClass$.MODULE$.ti())
                .rebalance()
                .addSink(new DiscardingSink<>());

        env.execute();
    }
}
