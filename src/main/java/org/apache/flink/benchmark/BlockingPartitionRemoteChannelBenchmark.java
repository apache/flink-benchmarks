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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.util.FileUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

@OperationsPerInvocation(value = BlockingPartitionRemoteChannelBenchmark.RECORDS_PER_INVOCATION)
public class BlockingPartitionRemoteChannelBenchmark extends RemoteBenchmarkBase {

    private static final int NUM_VERTICES = 2;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(BlockingPartitionRemoteChannelBenchmark.class.getCanonicalName())
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void remoteFilePartition(RemoteFileEnvironmentContext context) throws Exception {
        StreamGraph streamGraph =
                StreamGraphUtils.buildGraphForBatchJob(context.env, RECORDS_PER_INVOCATION);
        context.miniCluster.executeJobBlocking(
                StreamingJobGraphGenerator.createJobGraph(streamGraph));
    }

    @Benchmark
    public void remoteSortPartition(RemoteSortEnvironmentContext context) throws Exception {
        StreamGraph streamGraph =
                StreamGraphUtils.buildGraphForBatchJob(context.env, RECORDS_PER_INVOCATION);
        context.miniCluster.executeJobBlocking(
                StreamingJobGraphGenerator.createJobGraph(streamGraph));
    }

    /** Environment context for specific file based bounded blocking partition. */
    public static class BlockingPartitionEnvironmentContext extends RemoteBenchmarkContext {

        @Override
        public void setUp() throws Exception {
            super.setUp();

            env.setParallelism(PARALLELISM);
            env.setBufferTimeout(-1);
        }

        protected Configuration createConfiguration(boolean isSortShuffle) {
            Configuration configuration = super.createConfiguration();

            if (isSortShuffle) {
                configuration.setInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM, 1);
            } else {
                configuration.setInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM,
                        Integer.MAX_VALUE);
            }
            configuration.setString(
                    NettyShuffleEnvironmentOptions.NETWORK_BLOCKING_SHUFFLE_TYPE, "file");
            configuration.setString(
                    CoreOptions.TMP_DIRS,
                    FileUtils.getCurrentWorkingDirectory().toAbsolutePath().toString());
            return configuration;
        }

        @Override
        protected int getNumberOfVertices() {
            return NUM_VERTICES;
        }
    }

    public static class RemoteFileEnvironmentContext extends BlockingPartitionEnvironmentContext {
        @Override
        protected Configuration createConfiguration() {
            return createConfiguration(false);
        }
    }

    public static class RemoteSortEnvironmentContext extends BlockingPartitionEnvironmentContext {
        @Override
        protected Configuration createConfiguration() {
            return createConfiguration(true);
        }
    }
}
