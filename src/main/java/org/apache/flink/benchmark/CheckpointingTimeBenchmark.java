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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.benchmark.operators.RecordSource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TimeUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

/**
 * The test verifies that the debloating kicks in and properly downsizes buffers. In the end the
 * checkpoint should take ~2(number of rebalance) * DEBLOATING_TARGET.
 *
 * <p>Some info about the chosen numbers:
 *
 * <ul>
 *   <li>The minimal memory segment size is decreased (256b) so that the scaling possibility is
 *       higher. Memory segments start with 8kb
 *   <li>A memory segment of the minimal size fits ~9 records (of size 29b), each record takes
 *       ~200ns to be processed by the sink
 *   <li>We have 2 (exclusive buffers) * 4 (parallelism) + 8 floating = 64 buffers per gate, with
 *       300 ms debloating target and ~200ns/record processing speed, we can buffer 1500/64 = ~24
 *       records in a buffer after debloating which means the size of a buffer (24 * 29 = 696) is
 *       slightly above the minimal memory segment size.
 *   <li>The buffer debloating target of 300ms means a checkpoint should take ~2(number of
 *       exchanges)*300ms=~600ms
 * </ul>
 */
@OutputTimeUnit(SECONDS)
public class CheckpointingTimeBenchmark extends BenchmarkBase {
    public static final MemorySize DEBLOATING_RECORD_SIZE = MemorySize.parse("1b");
    public static final MemorySize UNALIGNED_RECORD_SIZE = MemorySize.parse("1kb");

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(CheckpointingTimeBenchmark.class.getCanonicalName())
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void checkpointSingleInput(SingleInputCheckpointEnvironmentContext context)
            throws Exception {
        final CompletableFuture<String> checkpoint =
                context.miniCluster.triggerCheckpoint(context.jobID);
        checkpoint.get();
    }

    @State(Scope.Thread)
    public static class SingleInputCheckpointEnvironmentContext
            extends CheckpointEnvironmentContext {
        public static final int NUM_OF_VERTICES = 3;
        public static final int FLATMAP_RECORDS = 5;

        @Param({"ALIGNED", "UNALIGNED", "UNALIGNED_1", "UNALIGNED_OVERDRAFT", "UNALIGNED_OVERDRAFT_1"})
        public CheckpointMode mode;

        @Param({"10 ms", "1 ms", "200 µs"})
        public String sleepTime;

        @Override
        protected CheckpointMode getMode() {
            return mode;
        }

        @Override
        protected StreamGraphWithSources getStreamGraph() {
            DataStreamSource<RecordSource.Record> source =
                    env.fromSource(
                            new RecordSource(Integer.MAX_VALUE, (int) getRecordSize().getBytes()),
                            noWatermarks(),
                            RecordSource.class.getName());

            source.slotSharingGroup("source")
                    .rebalance()
                    .flatMap(new FlatMapFunction<RecordSource.Record, RecordSource.Record>() {
                        @Override
                        public void flatMap(RecordSource.Record record, Collector<RecordSource.Record> collector) throws Exception {
                            for (int i = 0; i < FLATMAP_RECORDS; i++) {
                                collector.collect(record);
                            }
                        }
                    })
                    .slotSharingGroup("flatmap")
                    .rebalance()
                    .addSink(new SlowDiscardSink<>(TimeUtils.parseDuration(sleepTime).toNanos()))
                    .slotSharingGroup("sink");

            final StreamGraph streamGraph = env.getStreamGraph(false);
            final JobVertexID sourceId =
                    streamGraph
                            .getJobGraph()
                            .getVerticesSortedTopologicallyFromSources()
                            .get(0)
                            .getID();
            return new StreamGraphWithSources(streamGraph, Collections.singletonList(sourceId));
        }

        private MemorySize getRecordSize() {
            return mode == CheckpointMode.ALIGNED
                    ? CheckpointingTimeBenchmark.DEBLOATING_RECORD_SIZE
                    : CheckpointingTimeBenchmark.UNALIGNED_RECORD_SIZE;
        }

        @Override
        protected int getNumberOfTaskManagers() {
            return NUM_OF_VERTICES * JOB_PARALLELISM;
        }
    }
}
