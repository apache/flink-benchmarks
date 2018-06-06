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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * Abstract base class for sources with a defined number of events and a fixed key range.
 */
abstract public class BaseSourceWithKeyRange<T> implements ParallelSourceFunction<T> {
	private static final long serialVersionUID = 8318018060123048234L;

	protected final int numKeys;
	protected int remainingEvents;

	public BaseSourceWithKeyRange(int numEvents, int numKeys) {
		this.remainingEvents = numEvents;
		this.numKeys = numKeys;
	}

	protected void init() {
	}

	protected abstract T getElement(int keyId);

	@Override
	public void run(SourceContext<T> out) {
		init();

		int keyId = 0;
		while (--remainingEvents >= 0) {
			T element = getElement(keyId);
			synchronized (out.getCheckpointLock()) {
				out.collect(element);
			}
			++keyId;
			if (keyId >= numKeys) {
				keyId = 0;
			}
		}
	}

	@Override
	public void cancel() {
		this.remainingEvents = 0;
	}
}
