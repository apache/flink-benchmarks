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

package org.apache.flink.scheduler.benchmark.slot.matching.resolver;

import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SimpleSlotMatchingResolver;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotMatchingResolver;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotsBalancedSlotMatchingResolver;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TasksBalancedSlotMatchingResolver;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** The executor to drive {@link SlotMatchingResolver}. */
public class SlotMatchingResolverBenchmarkExecutor extends SchedulerBenchmarkExecutorBase {

    /**
     * We set the number of slots is very smaller than the number of task managers
     * to simulate the production environment to the greatest extent possible.
     */
    public static final int SLOTS_PER_TASKS_MANAGER = 8;
    public static final int TASK_MANAGERS = 128;

    private static final int requestedSlotSharingGroups = 3;
    private static final List<SlotSharingGroup> slotSharingGroups = new ArrayList<>();
    private static final Collection<ExecutionSlotSharingGroup> requestGroups = new ArrayList<>();
    private static final Collection<PhysicalSlot> slots = new ArrayList<>();

    static {
        // For ResourceProfile.UNKNOWN.
        slotSharingGroups.add(new SlotSharingGroup());
        // For other resource profiles.
        for (int i = 1; i < requestedSlotSharingGroups; i++) {
            SlotSharingGroup sharingGroup = new SlotSharingGroup();
            sharingGroup.setResourceProfile(newGrainfinedResourceProfile(i));
            slotSharingGroups.add(sharingGroup);
        }
        // For requested groups and slots.
        for (int tmIndex = 0; tmIndex < TASK_MANAGERS; tmIndex++) {

            TaskManagerLocation tml = getTaskManagerLocation(tmIndex + 1);

            for (int slotIndex = 0; slotIndex < SLOTS_PER_TASKS_MANAGER; slotIndex++) {
                ResourceProfile profile = newGrainfinedResourceProfile(slotIndex);

                slots.add(new TestingSlot(new AllocationID(), profile, tml));
                requestGroups.add(getExecutionSlotSharingGroup(slotIndex + 1, slotIndex));
            }
        }
    }

    private static ExecutionSlotSharingGroup getExecutionSlotSharingGroup(
            int loading, int slotIndex) {
        Set<ExecutionVertexID> executionVertexIDSet = new HashSet<>();
        JobVertexID jobVertexID = new JobVertexID();
        for (int i = 0; i < loading; i++) {
            executionVertexIDSet.add(new ExecutionVertexID(jobVertexID, i));
        }
        return new ExecutionSlotSharingGroup(
                slotSharingGroups.get(slotIndex % 3), executionVertexIDSet);
    }

    public static TaskManagerLocation getTaskManagerLocation(int dataPort) {
        try {
            InetAddress inetAddress = InetAddress.getByName("1.2.3.4");
            return new TaskManagerLocation(ResourceID.generate(), inetAddress, dataPort);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static ResourceProfile newGrainfinedResourceProfile(int slotIndex) {
        return ResourceProfile.newBuilder()
                .setCpuCores(slotIndex % 2 == 0 ? 1 : 2)
                .setTaskHeapMemoryMB(100)
                .setTaskOffHeapMemoryMB(100)
                .setManagedMemoryMB(100)
                .build();
    }

    @Param({"NONE", "SLOTS", "TASKS"})
    private TaskManagerLoadBalanceMode taskManagerLoadBalanceMode;

    private SlotMatchingResolver slotMatchingResolver;

    public static void main(String[] args) throws RunnerException {
        runBenchmark(SlotMatchingResolverBenchmarkExecutor.class);
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        slotMatchingResolver = getSlotMatchingResolver();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void runSlotsMatching(Blackhole blackhole) {
        blackhole.consume(
                slotMatchingResolver.matchSlotSharingGroupWithSlots(requestGroups, slots));
    }

    private SlotMatchingResolver getSlotMatchingResolver() {
        switch (taskManagerLoadBalanceMode) {
            case NONE:
                this.slotMatchingResolver = SimpleSlotMatchingResolver.INSTANCE;
                break;
            case SLOTS:
                this.slotMatchingResolver =
                        SlotsBalancedSlotMatchingResolver.INSTANCE;
                break;
            case TASKS:
                this.slotMatchingResolver =
                        TasksBalancedSlotMatchingResolver.INSTANCE;
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported task manager load balance mode '%s' in %s",
                                taskManagerLoadBalanceMode,
                                getClass().getName()));
        }
        return slotMatchingResolver;
    }
}
