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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The source should produce same records as {@link LongSource}.
 *
 * <p>{@link LongSource} generates records from 0 to {@code maxValue} for every parallel instance.
 * The original {@link NumberSequenceSource} would split the range 0 to {@code maxValue} between all subtasks.
 */
public class LongNewSource extends NumberSequenceSource {
	private final Boundedness boundedness;
	private final long maxValue;

	public LongNewSource(Boundedness boundedness, long maxValue) {
		super(-1, -1); // we do not use the from/to of the underlying source
		this.boundedness = boundedness;
		this.maxValue = maxValue;
	}

	@Override
	public Boundedness getBoundedness() {
		return boundedness;
	}

	@Override
	public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> createEnumerator(
			SplitEnumeratorContext<NumberSequenceSplit> splitEnumeratorContext) {

		final List<NumberSequenceSplit> splits =
				IntStream.range(0, splitEnumeratorContext.currentParallelism())
						.mapToObj(
								id -> new NumberSequenceSplit(String.valueOf(id), 0, maxValue)
						)
						.collect(Collectors.toList());
		return new IteratorSourceEnumerator<>(splitEnumeratorContext, splits);
	}
}
