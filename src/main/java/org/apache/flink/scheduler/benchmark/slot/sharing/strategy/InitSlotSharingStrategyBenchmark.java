package org.apache.flink.scheduler.benchmark.slot.sharing.strategy;

import org.apache.flink.configuration.TaskManagerOptions.TaskManagerLoadBalanceMode;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.LocalInputPreferredSlotSharingStrategy;
import org.apache.flink.runtime.scheduler.SlotSharingStrategy;
import org.apache.flink.runtime.scheduler.TaskBalancedPreferredSlotSharingStrategy;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import java.util.Collection;

/** The benchmark of initializing {@link SlotSharingStrategy}. */
public class InitSlotSharingStrategyBenchmark {

    private final JobGraph jobGraph;
    private final ExecutionGraph executionGraph;
    private final TaskManagerLoadBalanceMode taskManagerLoadBalanceMode;

    public InitSlotSharingStrategyBenchmark(
            TaskManagerLoadBalanceMode taskManagerLoadBalanceMode, Collection<JobVertex> vertices) {
        this.taskManagerLoadBalanceMode = taskManagerLoadBalanceMode;
        this.jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder().addJobVertices(vertices).build();
        try {
            this.executionGraph =
                    TestingDefaultExecutionGraphBuilder.newBuilder()
                            .setJobGraph(jobGraph)
                            .build(new DirectScheduledExecutorService());
        } catch (JobException | JobExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public SlotSharingStrategy createSlotSharingStrategy() {
        switch (taskManagerLoadBalanceMode) {
            case NONE:
                return new LocalInputPreferredSlotSharingStrategy.Factory()
                        .create(
                                executionGraph.getSchedulingTopology(),
                                jobGraph.getSlotSharingGroups(),
                                jobGraph.getCoLocationGroups());
            case TASKS:
                return new TaskBalancedPreferredSlotSharingStrategy.Factory()
                        .create(
                                executionGraph.getSchedulingTopology(),
                                jobGraph.getSlotSharingGroups(),
                                jobGraph.getCoLocationGroups());
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported task manager load balance mode '%s' in %s",
                                taskManagerLoadBalanceMode,
                                getClass().getName()));
        }
    }
}
