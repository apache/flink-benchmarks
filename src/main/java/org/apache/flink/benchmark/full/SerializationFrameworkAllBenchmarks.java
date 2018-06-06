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

package org.apache.flink.benchmark.full;

import org.apache.flink.benchmark.SerializationFrameworkMiniBenchmarks;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

/**
 * Benchmark for serializing POJOs and Tuples with different serialization frameworks.
 */
public class SerializationFrameworkAllBenchmarks extends SerializationFrameworkMiniBenchmarks {

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + SerializationFrameworkAllBenchmarks.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerPojoWithoutRegistration() throws Exception {
		LocalStreamEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironment(4);

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerKryoWithoutRegistration() throws Exception {
		LocalStreamEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironment(4);
		env.getConfig().enableForceKryo();

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerAvroReflect() throws Exception {
		LocalStreamEnvironment env =
				StreamExecutionEnvironment.createLocalEnvironment(4);
		env.getConfig().enableForceAvro();

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}
}
