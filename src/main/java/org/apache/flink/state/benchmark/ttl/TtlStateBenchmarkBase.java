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

package org.apache.flink.state.benchmark.ttl;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.benchmark.StateBenchmarkBase;
import org.openjdk.jmh.annotations.Param;

import java.time.Duration;

/** The base class for state tests with ttl. */
public class TtlStateBenchmarkBase extends StateBenchmarkBase {

    private static final long initialTime = 1000000;

    /** The expired time of ttl. */
    public enum ExpiredTimeOptions {

        /** Expire 3 percent of the initial keys per iteration. */
        Expire3PercentPerIteration(3),

        /** never expired but enable the ttl. */
        NeverExpired(0);

        public long advanceTimePerIteration;
        ExpiredTimeOptions(int expirePercentPerIteration) {
            this.advanceTimePerIteration = initialTime * expirePercentPerIteration / 100;
        }
    }

    @Param({"Expire3PercentPerIteration", "NeverExpired"})
    protected ExpiredTimeOptions expiredOption;

    @Param({"OnCreateAndWrite", "OnReadAndWrite"})
    protected StateTtlConfig.UpdateType updateType;

    @Param({"NeverReturnExpired", "ReturnExpiredIfNotCleanedUp"})
    protected StateTtlConfig.StateVisibility stateVisibility;

    protected ControllableTtlTimeProvider timeProvider;

    /** Configure the state descriptor with ttl. */
    protected <T extends StateDescriptor<?, ?>> T configTtl(T stateDescriptor) {
        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(Duration.ofMillis(initialTime))
                        .setUpdateType(updateType)
                        .setStateVisibility(stateVisibility)
                        .build();
        stateDescriptor.enableTimeToLive(ttlConfig);
        return stateDescriptor;
    }

    @Override
    protected KeyedStateBackend<Long> createKeyedStateBackend() throws Exception {
        timeProvider = new ControllableTtlTimeProvider();
        return createKeyedStateBackend(timeProvider);
    }

    protected void setTtlWhenInitialization() {
        timeProvider.setCurrentTimestamp(random.nextLong(initialTime + 1));
    }

    protected void finishInitialization() {
        timeProvider.setCurrentTimestamp(initialTime);
    }

    protected void advanceTimePerIteration() {
        timeProvider.advanceTimestamp(expiredOption.advanceTimePerIteration);
    }

    static class ControllableTtlTimeProvider implements TtlTimeProvider {

        long current = 0L;

        @Override
        public long currentTimestamp() {
            return current;
        }

        public void setCurrentTimestamp(long value) {
            current = value;
        }

        public void advanceTimestamp(long value) {
            current += value;
        }
    }
}
