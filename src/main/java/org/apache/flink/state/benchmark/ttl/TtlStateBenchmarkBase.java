package org.apache.flink.state.benchmark.ttl;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.state.benchmark.StateBenchmarkBase;
import org.openjdk.jmh.annotations.Param;

import java.util.concurrent.TimeUnit;

/** The base class for state tests with ttl. */
public class TtlStateBenchmarkBase extends StateBenchmarkBase {

    /** The expired time of ttl. */
    public enum ExpiredTimeOptions {

        /** 5 seconds. */
        Seconds5(5000),

        /** never expired but enable the ttl. */
        MaxTime(Long.MAX_VALUE);

        private Time time;
        ExpiredTimeOptions(long mills) {
            time = Time.of(mills, TimeUnit.MILLISECONDS);
        }
    }

    @Param({"Seconds5", "MaxTime"})
    protected ExpiredTimeOptions expiredTime;

    @Param({"OnCreateAndWrite", "OnReadAndWrite"})
    protected StateTtlConfig.UpdateType updateType;

    @Param({"ReturnExpiredIfNotCleanedUp", "NeverReturnExpired"})
    protected StateTtlConfig.StateVisibility stateVisibility;

    /** Configure the state descriptor with ttl. */
    protected <T extends StateDescriptor<?, ?>> T configTtl(T stateDescriptor) {
        StateTtlConfig ttlConfig =
                new StateTtlConfig.Builder(expiredTime.time)
                        .setUpdateType(updateType)
                        .setStateVisibility(stateVisibility)
                        .build();
        stateDescriptor.enableTimeToLive(ttlConfig);
        return stateDescriptor;
    }
}
