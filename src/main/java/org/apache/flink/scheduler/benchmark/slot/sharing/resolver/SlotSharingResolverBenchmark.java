package org.apache.flink.scheduler.benchmark.slot.sharing.resolver;

import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptive.JobGraphJobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.DefaultSlotSharingResolver;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingResolver;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TaskBalancedSlotSharingResolver;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.SchedulerBase.computeVertexParallelismStore;

/** The benchmark of initializing {@link org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingResolver}. */
public class SlotSharingResolverBenchmark {

    private final JobInformation jobInformation;
    private final VertexParallelism vertexParallelism;
    private final TaskManagerLoadBalanceMode taskManagerLoadBalanceMode;

    public SlotSharingResolverBenchmark(
            TaskManagerLoadBalanceMode taskManagerLoadBalanceMode, Collection<JobVertex> vertices) {
        this.taskManagerLoadBalanceMode = taskManagerLoadBalanceMode;
        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder().addJobVertices(vertices).build();
        try {
            ExecutionGraph executionGraph =
                    TestingDefaultExecutionGraphBuilder.newBuilder()
                            .setJobGraph(jobGraph)
                            .build(new DirectScheduledExecutorService());
            VertexParallelismStore vertexParallelismStore = computeVertexParallelismStore(jobGraph);
            this.jobInformation = new JobGraphJobInformation(jobGraph, vertexParallelismStore);
            this.vertexParallelism = new VertexParallelism(
                    executionGraph.getAllVertices().values().stream()
                            .collect(
                                    Collectors.toMap(
                                            AccessExecutionJobVertex::getJobVertexId,
                                            AccessExecutionJobVertex::getParallelism)));
        } catch (JobException | JobExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup> invokeSlotSharingResolver() {
        SlotSharingResolver slotSharingResolver = createSlotSharingResolver();
        return slotSharingResolver.getExecutionSlotSharingGroups(jobInformation, vertexParallelism);
    }

    private SlotSharingResolver createSlotSharingResolver() {
        switch (taskManagerLoadBalanceMode) {
            case NONE:
                return DefaultSlotSharingResolver.INSTANCE;
            case TASKS:
                return TaskBalancedSlotSharingResolver.INSTANCE;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported task manager load balance mode '%s' in %s",
                                taskManagerLoadBalanceMode,
                                getClass().getName()));
        }
    }
}
