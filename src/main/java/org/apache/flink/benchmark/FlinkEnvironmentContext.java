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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterPipelineExecutorServiceLoader;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
public class FlinkEnvironmentContext {

    public static final int NUM_NETWORK_BUFFERS = 1000;
    protected final int parallelism = 1;
    protected final boolean objectReuse = true;
    public StreamExecutionEnvironment env;
    public MiniCluster miniCluster;

    @Setup
    public void setUp() throws Exception {
        if (miniCluster != null) {
            throw new RuntimeException("setUp was called multiple times!");
        }
        final Configuration clusterConfig = createConfiguration();
        miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setNumSlotsPerTaskManager(getNumberOfSlotsPerTaskManager())
                                .setNumTaskManagers(getNumberOfTaskManagers())
                                .setConfiguration(clusterConfig)
                                .build());

        try {
            miniCluster.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // set up the execution environment
        env =
                new StreamExecutionEnvironment(
                        new MiniClusterPipelineExecutorServiceLoader(miniCluster),
                        clusterConfig,
                        null);

        env.setParallelism(parallelism);
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        }
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStateBackend(new MemoryStateBackend());
    }

    @TearDown
    public void tearDown() throws Exception {
        miniCluster.close();
        miniCluster = null;
    }

    protected int getNumberOfTaskManagers() {
        return 1;
    }

    protected int getNumberOfSlotsPerTaskManager() {
        return 4;
    }

    public void execute() throws Exception {
        env.execute();
    }

    protected Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "0");
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, NUM_NETWORK_BUFFERS);
        configuration.set(DeploymentOptions.TARGET, MiniClusterPipelineExecutorServiceLoader.NAME);
        configuration.set(DeploymentOptions.ATTACHED, true);
        return configuration;
    }
}
