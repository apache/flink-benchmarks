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

/** Benchmark base for setting up the cluster to perform remote network shuffle. */
public abstract class RemoteBenchmarkBase extends BenchmarkBase {

    protected static final int PARALLELISM = 4;
    protected static final int RECORDS_PER_SUBTASK = 10_000_000;
    protected static final int RECORDS_PER_INVOCATION = RECORDS_PER_SUBTASK * PARALLELISM;

    public abstract static class RemoteBenchmarkContext extends FlinkEnvironmentContext {
        @Override
        protected int getNumberOfTaskManagers() {
            return getNumberOfVertices() * PARALLELISM;
        }

        @Override
        protected int getNumberOfSlotsPerTaskManager() {
            return 1;
        }

        /** @return the number of vertices the respective job graph contains. */
        abstract int getNumberOfVertices();
    }
}
