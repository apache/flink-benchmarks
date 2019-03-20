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

package org.apache.flink.state.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

/**
 * Constants for state benchmark tests. Also generates random keys/values in advance to avoid
 * possible affect of using {@link Random#nextLong()}
 */
class StateBenchmarkConstants {
    static final int mapKeyCount = 10;
    static final int listValueCount = 100;
    static final int setupKeyCount = 500_000;
    static final String rootDirName = "benchmark";
    static final String recoveryDirName = "localRecovery";
    static final String dbDirName = "dbPath";

    static final ArrayList<Long> mapKeys = new ArrayList<>(mapKeyCount);

    static {
        for (int i = 0; i < mapKeyCount; i++) {
            mapKeys.add((long) i);
        }
        Collections.shuffle(mapKeys);
    }

    static final ArrayList<Double> mapValues = new ArrayList<>(mapKeyCount);

    static {
        Random random = new Random();
        for (int i = 0; i < mapKeyCount; i++) {
            mapValues.add(random.nextDouble());
        }
        Collections.shuffle(mapValues);
    }

    static final ArrayList<Long> setupKeys = new ArrayList<>(setupKeyCount);

    static {
        for (long i = 0; i < setupKeyCount; i++) {
            setupKeys.add(i);
        }
        Collections.shuffle(setupKeys);
    }

    static final int newKeyCount = 500_000;
    static final ArrayList<Long> newKeys = new ArrayList<>(newKeyCount);

    static {
        for (long i = 0; i < newKeyCount; i++) {
            newKeys.add(i + setupKeyCount);
        }
        Collections.shuffle(newKeys);
    }

    static final int randomValueCount = 1_000_000;
    static final ArrayList<Long> randomValues = new ArrayList<>(randomValueCount);

    static {
        for (long i = 0; i < randomValueCount; i++) {
            randomValues.add(i);
        }
        Collections.shuffle(randomValues);
    }
}
