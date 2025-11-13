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

package org.apache.flink.scheduler.benchmark.slot.matching.strategy;

import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PendingRequest;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.RequestSlotMatchingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SimpleRequestSlotMatchingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.TasksBalancedRequestSlotMatchingStrategy;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlot;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.scheduler.benchmark.SchedulerBenchmarkExecutorBase;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import static org.apache.flink.scheduler.benchmark.slot.matching.resolver.SlotMatchingResolverBenchmarkExecutor.SLOTS_PER_TASKS_MANAGER;
import static org.apache.flink.scheduler.benchmark.slot.matching.resolver.SlotMatchingResolverBenchmarkExecutor.TASK_MANAGERS;
import static org.apache.flink.scheduler.benchmark.slot.matching.resolver.SlotMatchingResolverBenchmarkExecutor.getTaskManagerLocation;
import static org.apache.flink.scheduler.benchmark.slot.matching.resolver.SlotMatchingResolverBenchmarkExecutor.newGrainfinedResourceProfile;

/** The executor to drive {@link RequestSlotMatchingStrategy}. */
public class RequestSlotMatchingStrategyBenchmarkExecutor
        extends SchedulerBenchmarkExecutorBase {

    private static final Collection<PhysicalSlot> slots = new ArrayList<>();
    private static final Collection<PendingRequest> slotRequests = new ArrayList<>();

    static {
        // For requested groups and slots.
        for (int tmIndex = 0; tmIndex < TASK_MANAGERS; tmIndex++) {

            TaskManagerLocation tml = getTaskManagerLocation(tmIndex + 1);

            for (int slotIndex = 0; slotIndex < SLOTS_PER_TASKS_MANAGER; slotIndex++) {
                ResourceProfile profile = newGrainfinedResourceProfile(slotIndex);

                slots.add(new TestingSlot(new AllocationID(), profile, tml));
                slotRequests.add(getPendingRequest(slotIndex + 1, slotIndex));
            }
        }
    }

    private static PendingRequest getPendingRequest(float loading, int slotIndex) {
        return PendingRequest.createNormalRequest(
                new SlotRequestId(),
                newGrainfinedResourceProfile(slotIndex),
                new DefaultLoadingWeight(loading),
                Collections.emptyList());
    }

    @Param({"NONE", "TASKS"})
    private TaskManagerLoadBalanceMode taskManagerLoadBalanceMode;

    private RequestSlotMatchingStrategy requestSlotMatchingStrategy;

    public static void main(String[] args) throws RunnerException {
        runBenchmark(RequestSlotMatchingStrategyBenchmarkExecutor.class);
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        requestSlotMatchingStrategy = getRequestSlotMatchingStrategy();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void runSlotsMatching(Blackhole blackhole) {
        blackhole.consume(
                requestSlotMatchingStrategy.matchRequestsAndSlots(
                        slots, slotRequests, new HashMap<>()));
    }

    private RequestSlotMatchingStrategy getRequestSlotMatchingStrategy() {
        switch (taskManagerLoadBalanceMode) {
            case TASKS:
                this.requestSlotMatchingStrategy =
                        TasksBalancedRequestSlotMatchingStrategy.INSTANCE;
                break;
            case NONE:
                this.requestSlotMatchingStrategy = SimpleRequestSlotMatchingStrategy.INSTANCE;
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported task manager load balance mode '%s' in %s",
                                taskManagerLoadBalanceMode,
                                getClass().getName()));
        }
        return requestSlotMatchingStrategy;
    }
}
