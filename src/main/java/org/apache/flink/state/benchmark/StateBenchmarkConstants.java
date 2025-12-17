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
public class StateBenchmarkConstants {
    // TODO: why all of those static fields? Those should be inside a context class
    public static final int MAP_KEY_COUNT = 10;
    public static final int LIST_VALUE_COUNT = 100;
    public static final int SETUP_KEY_COUNT = 500_000;
    public static final String ROOT_DIR_NAME = "benchmark";
    public static final String RECOVERY_DIR_NAME = "localRecovery";
    public static final String DB_PATH = "dbPath";

    public static final ArrayList<Long> MAP_KEYS = new ArrayList<>(MAP_KEY_COUNT);
    public static final ArrayList<Double> MAP_VALUES = new ArrayList<>(MAP_KEY_COUNT);
    public static final ArrayList<Long> SETUP_KEYS = new ArrayList<>(SETUP_KEY_COUNT);
    public static final int NEW_KEY_COUNT = 500_000;
    public static final ArrayList<Long> NEW_KEYS = new ArrayList<>(NEW_KEY_COUNT);
    public static final int RANDOM_VALUE_COUNT = 1_000_000;
    public static final ArrayList<Long> RANDOM_VALUES = new ArrayList<>(RANDOM_VALUE_COUNT);

    static {
        for (int i = 0; i < MAP_KEY_COUNT; i++) {
            MAP_KEYS.add((long) i);
        }
        Collections.shuffle(MAP_KEYS);
    }

    static {
        Random random = new Random();
        for (int i = 0; i < MAP_KEY_COUNT; i++) {
            MAP_VALUES.add(random.nextDouble());
        }
        Collections.shuffle(MAP_VALUES);
    }

    static {
        for (long i = 0; i < SETUP_KEY_COUNT; i++) {
            SETUP_KEYS.add(i);
        }
        Collections.shuffle(SETUP_KEYS);
    }

    static {
        for (long i = 0; i < NEW_KEY_COUNT; i++) {
            NEW_KEYS.add(i + SETUP_KEY_COUNT);
        }
        Collections.shuffle(NEW_KEYS);
    }

    static {
        for (long i = 0; i < RANDOM_VALUE_COUNT; i++) {
            RANDOM_VALUES.add(i);
        }
        Collections.shuffle(RANDOM_VALUES);
    }
}
