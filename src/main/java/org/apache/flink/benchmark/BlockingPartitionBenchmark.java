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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.FileUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

/** JMH throughput benchmark runner. */
@OperationsPerInvocation(value = BlockingPartitionBenchmark.RECORDS_PER_INVOCATION)
public class BlockingPartitionBenchmark extends BenchmarkBase {

    public static final int RECORDS_PER_INVOCATION = 15_000_000;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + BlockingPartitionBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void uncompressedFilePartition(UncompressedFileEnvironmentContext context)
            throws Exception {
        executeBenchmark(context.env);
    }

    @Benchmark
    public void compressedFilePartition(CompressedFileEnvironmentContext context) throws Exception {
        executeBenchmark(context.env);
    }

    @Benchmark
    public void uncompressedMmapPartition(UncompressedMmapEnvironmentContext context)
            throws Exception {
        executeBenchmark(context.env);
    }

    @Benchmark
    public void compressedSortPartition(CompressedSortEnvironmentContext context) throws Exception {
        executeBenchmark(context.env);
    }

    @Benchmark
    public void uncompressedSortPartition(UncompressedSortEnvironmentContext context) throws Exception {
        executeBenchmark(context.env);
    }

    private void executeBenchmark(StreamExecutionEnvironment env) throws Exception {
        StreamGraph streamGraph =
                StreamGraphUtils.buildGraphForBatchJob(env, RECORDS_PER_INVOCATION);
        env.execute(streamGraph);
    }

    /** Setup for the benchmark(s). */
    public static class BlockingPartitionEnvironmentContext extends FlinkEnvironmentContext {

        /**
         * Parallelism of 1 causes the reads/writes to be always sequential and only covers the case
         * of one reader. More parallelism should be more suitable for finding performance
         * regressions of the code. Considering that the benchmarking machine has 4 CPU cores, we
         * set the parallelism to 4.
         */
        private final int parallelism = 4;

        @Override
        public void setUp() throws Exception {
            super.setUp();

            env.setParallelism(parallelism);
            env.setBufferTimeout(-1);
        }

        protected Configuration createConfiguration(
                boolean compressionEnabled, String subpartitionType, boolean isSortShuffle) {
            Configuration configuration = super.createConfiguration();

            if (isSortShuffle) {
                configuration.setInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM, 1);
            } else {
                configuration.setInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM,
                        Integer.MAX_VALUE);
            }
            configuration.setBoolean(
                    NettyShuffleEnvironmentOptions.BATCH_SHUFFLE_COMPRESSION_ENABLED,
                    compressionEnabled);
            configuration.setString(
                    NettyShuffleEnvironmentOptions.NETWORK_BLOCKING_SHUFFLE_TYPE, subpartitionType);
            configuration.setString(
                    CoreOptions.TMP_DIRS,
                    FileUtils.getCurrentWorkingDirectory().toAbsolutePath().toString());
            return configuration;
        }
    }

    public static class UncompressedFileEnvironmentContext
            extends BlockingPartitionEnvironmentContext {
        @Override
        protected Configuration createConfiguration() {
            return createConfiguration(false, "file", false);
        }
    }

    public static class CompressedFileEnvironmentContext
            extends BlockingPartitionEnvironmentContext {
        @Override
        protected Configuration createConfiguration() {
            return createConfiguration(true, "file", false);
        }
    }

    public static class UncompressedMmapEnvironmentContext
            extends BlockingPartitionEnvironmentContext {
        @Override
        protected Configuration createConfiguration() {
            return createConfiguration(false, "mmap", false);
        }
    }

    public static class CompressedSortEnvironmentContext
            extends BlockingPartitionEnvironmentContext {
        @Override
        protected Configuration createConfiguration() {
            return createConfiguration(true, "file", true);
        }
    }

    public static class UncompressedSortEnvironmentContext
            extends BlockingPartitionEnvironmentContext {
        @Override
        protected Configuration createConfiguration() {
            return createConfiguration(false, "file", true);
        }
    }
}
