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
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;

import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
public class FlinkEnvironmentContext {

    public static final int NUM_NETWORK_BUFFERS = 1000;

    public StreamExecutionEnvironment env;

    protected final int parallelism = 1;
    protected final boolean objectReuse = true;

    @Setup
    public void setUp() throws IOException {
        // set up the execution environment
        env = getStreamExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().disableSysoutLogging();
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        }

        env.setStateBackend(new MemoryStateBackend());
    }

    public void execute() throws Exception {
        env.execute();
    }

    protected Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, NUM_NETWORK_BUFFERS);
        return configuration;
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return StreamExecutionEnvironment.createLocalEnvironment(1, createConfiguration());
    }
}
