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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.benchmark.functions.LongSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

@OperationsPerInvocation(value = RemoteChannelThroughputBenchmark.RECORDS_PER_INVOCATION)
public class RemoteChannelThroughputBenchmark extends RemoteBenchmarkBase {
    private static final int NUM_VERTICES = 3;
    private static final long CHECKPOINT_INTERVAL_MS = 100;

    @Param({"ALIGNED", "UNALIGNED"})
    public String mode = "ALIGNED";

    public static void main(String[] args)
            throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(RemoteChannelThroughputBenchmark.class.getCanonicalName())
                .build();

        new Runner(options).run();
    }

    @Override
    public int getNumberOfVertexes() {
        return NUM_VERTICES;
    }

    @Benchmark
    public void remoteRebalance(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        env.setParallelism(PARALLELISM);
        env.getCheckpointConfig().enableUnalignedCheckpoints(!mode.equals("ALIGNED"));

        DataStreamSource<Long> source = env.addSource(new LongSource(RECORDS_PER_SUBTASK));
        source
            .slotSharingGroup("source").rebalance()
            .map((MapFunction<Long, Long>) value -> value).slotSharingGroup("map").rebalance()
            .addSink(new DiscardingSink<>()).slotSharingGroup("sink");

        miniCluster.executeJobBlocking(StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph()));
    }
}
