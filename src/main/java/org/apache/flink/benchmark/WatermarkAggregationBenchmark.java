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

import org.apache.flink.runtime.source.coordinator.SourceCoordinatorAlignmentBenchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

/** The watermark aggregation benchmark for source coordinator when enabling the watermark alignment. */
public class WatermarkAggregationBenchmark extends BenchmarkBase {

    private static final int NUM_SUBTASKS = 5000;

    private static final int ROUND_PER_INVOCATION = 10;

    private SourceCoordinatorAlignmentBenchmark benchmark;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + WatermarkAggregationBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        benchmark = new SourceCoordinatorAlignmentBenchmark();
        benchmark.setup(NUM_SUBTASKS);
    }

    @Benchmark
    @OperationsPerInvocation(NUM_SUBTASKS * ROUND_PER_INVOCATION)
    public void aggregateWatermark() {
        for (int round = 0; round < ROUND_PER_INVOCATION; round++) {
            benchmark.sendReportedWatermarkToAllSubtasks();
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        benchmark.teardown();
    }

}
