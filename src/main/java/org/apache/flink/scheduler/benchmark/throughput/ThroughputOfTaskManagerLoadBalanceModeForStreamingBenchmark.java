/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.scheduler.benchmark.throughput;

import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.benchmark.FlinkEnvironmentContext;
import org.apache.flink.benchmark.functions.LongSourceType;
import org.apache.flink.benchmark.functions.MultiplyByTwo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

@OperationsPerInvocation(
        value = ThroughputOfTaskManagerLoadBalanceModeForStreamingBenchmark.RECORDS_PER_INVOCATION)
public class ThroughputOfTaskManagerLoadBalanceModeForStreamingBenchmark extends BenchmarkBase {
    public static final int RECORDS_PER_INVOCATION = 150_000;
    private static final long CHECKPOINT_INTERVAL_MS = 100;

    @Param({"F27_UNBOUNDED"})
    public LongSourceType sourceType;

    @Param({"NONE", "TASKS"})
    public TaskManagerLoadBalanceMode taskManagerLoadBalanceMode;

    @Param({"true", "false"})
    public boolean allParallelismsOfVerticesSame;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(
                                ".*"
                                        + ThroughputOfTaskManagerLoadBalanceModeForStreamingBenchmark
                                                .class
                                                .getCanonicalName()
                                        + ".*")
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void throughput(InputBenchmarkFlinkEnvironmentContext context) throws Exception {

        int[] parallelisms = new int[] {1, 2, 3, 4, 5, 6};
        StreamExecutionEnvironment env = context.env;
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        env.disableOperatorChaining();
        configAdaptivePartitioner(env);

        DataStreamSource<Long> source = sourceType.source(env, RECORDS_PER_INVOCATION);
        if (allParallelismsOfVerticesSame) {
            source.setParallelism(parallelisms[parallelisms.length - 1]);
        } else {
            source.setParallelism(parallelisms[0]);
        }

        SingleOutputStreamOperator<Long> index = source;
        for (int i = 1; i < parallelisms.length; i++) {
            index =
                    index.map(new MultiplyByTwo())
                            .setParallelism(
                                    allParallelismsOfVerticesSame
                                            ? parallelisms[parallelisms.length - 1]
                                            : parallelisms[i]);
        }

        index.sinkTo(new DiscardingSink<>()).setParallelism(parallelisms[parallelisms.length - 1]);

        env.execute();
    }

    private void configAdaptivePartitioner(StreamExecutionEnvironment env) {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.TASK_MANAGER_LOAD_BALANCE_MODE, taskManagerLoadBalanceMode);
        config.setString("restart-strategy", "fixed-delay");
        config.setString("restart-strategy.fixed-delay.attempts", "15000000");
        config.setString("restart-strategy.fixed-delay.delay", "3s");
        env.configure(config);
    }

    public static class InputBenchmarkFlinkEnvironmentContext extends FlinkEnvironmentContext {

        @Override
        protected int getNumberOfTaskManagers() {
            return 2;
        }

        @Override
        protected int getNumberOfSlotsPerTaskManager() {
            return 6;
        }
    }
}
