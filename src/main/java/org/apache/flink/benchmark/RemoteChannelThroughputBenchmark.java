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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

@OperationsPerInvocation(value = RemoteChannelThroughputBenchmark.RECORDS_PER_INVOCATION)
public class RemoteChannelThroughputBenchmark extends RemoteBenchmarkBase {
    private static final String ALIGNED = "ALIGNED";
    private static final String DEBLOAT = "DEBLOAT";
    private static final String UNALIGNED = "UNALIGNED";

    private static final int NUM_VERTICES = 3;
    private static final long CHECKPOINT_INTERVAL_MS = 100;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(RemoteChannelThroughputBenchmark.class.getCanonicalName())
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void remoteRebalance(RemoteChannelThroughputBenchmarkContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        env.setParallelism(PARALLELISM);
        env.getCheckpointConfig().enableUnalignedCheckpoints(context.mode.equals(UNALIGNED));

        DataStreamSource<Long> source = env.addSource(new LongSource(RECORDS_PER_SUBTASK));
        source.slotSharingGroup("source")
                .rebalance()
                .map((MapFunction<Long, Long>) value -> value)
                .slotSharingGroup("map")
                .rebalance()
                .addSink(new DiscardingSink<>())
                .slotSharingGroup("sink");

        context.miniCluster.executeJobBlocking(
                StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph()));
    }

    @State(Scope.Thread)
    public static class RemoteChannelThroughputBenchmarkContext extends RemoteBenchmarkContext {
        @Param({ALIGNED, UNALIGNED, DEBLOAT})
        public String mode = ALIGNED;

        @Override
        protected Configuration createConfiguration() {
            Configuration configuration = super.createConfiguration();
            if (mode.equals(DEBLOAT)) {
                configuration.setBoolean(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, true);
            }
            return configuration;
        }

        @Override
        protected int getNumberOfVertices() {
            return NUM_VERTICES;
        }
    }
}
