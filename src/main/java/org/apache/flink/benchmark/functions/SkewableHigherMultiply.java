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

package org.apache.flink.benchmark.functions;

import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SkewableHigherMultiply extends RichMapFunction<Long, Long> {

    private final int additionalCostMillisPerBatch;
    private final int recordsPerBatch;
    private int numRecordsForCurrentBatch;
    private final Set<Integer> expectedSkewedSubTasksIndexes;

    public SkewableHigherMultiply(
            int additionalCostMillisPerBatch,
            int recordsPerBatch,
            Integer... expectedSkewedSubTasksIndexes) {
        this.additionalCostMillisPerBatch = additionalCostMillisPerBatch;
        this.recordsPerBatch = recordsPerBatch;
        this.expectedSkewedSubTasksIndexes =
                new HashSet<>(Arrays.asList(expectedSkewedSubTasksIndexes));
    }

    @Override
    public Long map(Long value) throws Exception {
        double base = value * 2.0d;
        if (additionalCostMillisPerBatch <= 0) {
            return (long) base;
        } else {
            if (expectedSkewedSubTasksIndexes.contains(
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask())) {
                if (++numRecordsForCurrentBatch == recordsPerBatch) {
                    numRecordsForCurrentBatch = 0;
                    Thread.sleep(additionalCostMillisPerBatch);
                }
            }
            return (long) base;
        }
    }
}
