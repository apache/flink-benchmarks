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

import org.apache.flink.streaming.runtime.io.benchmark.StreamNetworkPointToPointBenchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/** JMH latency benchmark runner. */
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
public class StreamNetworkLatencyBenchmarkExecutor extends BenchmarkBase {

    private static final int RECORDS_PER_INVOCATION = 100;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(
                                ".*"
                                        + StreamNetworkLatencyBenchmarkExecutor.class
                                                .getCanonicalName()
                                        + ".*")
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void networkLatency1to1(Environment context) throws Exception {
        context.executeBenchmark(RECORDS_PER_INVOCATION, false);
    }

    /** Setup for the benchmark(s). */
    @State(Thread)
    public static class Environment extends StreamNetworkPointToPointBenchmark {
        @Setup
        public void setUp() throws Exception {
            super.setUp(10);
        }

        @TearDown
        public void tearDown() {
            super.tearDown();
        }
    }
}
