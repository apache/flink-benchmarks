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

public class QueuingLongSource extends LongSource {

    private static Object lock = new Object();

    private static int currentRank = 1;

    private final int rank;

    public QueuingLongSource(int rank, long maxValue) {
        super(maxValue);
        this.rank = rank;
    }

    public static void reset() {
        currentRank = 1;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        synchronized (lock) {
            while (currentRank != rank) {
                lock.wait();
            }
        }

        super.run(ctx);

        synchronized (lock) {
            currentRank++;
            lock.notifyAll();
        }
    }
}
