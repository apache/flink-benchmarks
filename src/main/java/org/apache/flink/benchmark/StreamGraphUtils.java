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

package org.apache.flink.benchmark;

import org.apache.flink.benchmark.functions.LongSource;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;

/**
 * Utilities for building respective graph for performing in benchmark.
 */
public class StreamGraphUtils {

	public static StreamGraph buildGraphForBatchJob(StreamExecutionEnvironment env, int numRecords) {
		DataStreamSource<Long> source = env.addSource(new LongSource(numRecords));
		source.addSink(new DiscardingSink<>());

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setChaining(false);
		streamGraph.setGlobalDataExchangeMode(GlobalDataExchangeMode.ALL_EDGES_BLOCKING);
		streamGraph.setJobType(JobType.BATCH);

		return streamGraph;
	}
}
