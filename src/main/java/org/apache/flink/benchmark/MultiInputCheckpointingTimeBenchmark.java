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

import org.apache.flink.benchmark.operators.RecordSource;
import org.apache.flink.benchmark.operators.RecordSource.Record;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

/**
 * The test verifies that the debloating kicks in and properly downsizes buffers in case of multi
 * input gates with different throughput. In the end the checkpoint should take ~1(number of
 * rebalance) * DEBLOATING_TARGET.
 */
@OutputTimeUnit(SECONDS)
public class MultiInputCheckpointingTimeBenchmark extends BenchmarkBase {

    public static final MemorySize SMALL_RECORD_SIZE = MemorySize.parse("1b");
    public static final MemorySize BIG_RECORD_SIZE = MemorySize.parse("1kb");

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(MultiInputCheckpointingTimeBenchmark.class.getCanonicalName())
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void checkpointMultiInput(MultiInputCheckpointEnvironmentContext context)
            throws Exception {
        final CompletableFuture<String> checkpoint =
                context.miniCluster.triggerCheckpoint(context.jobID);
        checkpoint.get();
    }

    @State(Scope.Thread)
    public static class MultiInputCheckpointEnvironmentContext
            extends CheckpointEnvironmentContext {

        private static final int NUM_OF_VERTICES = 3;

        @Override
        protected CheckpointMode getMode() {
            return CheckpointMode.ALIGNED;
        }

        @Override
        protected StreamGraphWithSources getStreamGraph() {
            DataStream<Record> source1 =
                    env.fromSource(
                                    new RecordSource(
                                            Integer.MAX_VALUE, (int) SMALL_RECORD_SIZE.getBytes()),
                                    noWatermarks(),
                                    RecordSource.class.getName())
                            .slotSharingGroup("source-small-records")
                            .rebalance();

            DataStream<Record> source2 =
                    env.fromSource(
                                    new RecordSource(
                                            Integer.MAX_VALUE, (int) BIG_RECORD_SIZE.getBytes()),
                                    noWatermarks(),
                                    RecordSource.class.getName())
                            .slotSharingGroup("source-big-records")
                            .rebalance();

            source1.connect(source2)
                    .map(
                            new CoMapFunction<Record, Record, Record>() {
                                @Override
                                public Record map1(Record record) throws Exception {
                                    return record;
                                }

                                @Override
                                public Record map2(Record record) throws Exception {
                                    return record;
                                }
                            })
                    .name("co-map")
                    .slotSharingGroup("map-and-sink")
                    .addSink(new SlowDiscardSink<>())
                    .slotSharingGroup("map-and-sink");

            final StreamGraph streamGraph = env.getStreamGraph(false);
            final JobGraph jobGraph = streamGraph.getJobGraph();
            final List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

            return new StreamGraphWithSources(
                    streamGraph, Arrays.asList(vertices.get(0).getID(), vertices.get(1).getID()));
        }

        @Override
        protected int getNumberOfTaskManagers() {
            return NUM_OF_VERTICES * JOB_PARALLELISM;
        }
    }
}
