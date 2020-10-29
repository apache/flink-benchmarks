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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.SplittableIterator;

import org.junit.Assert;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * An end to end test for sorted inputs for a keyed operator with bounded inputs.
 */
public class SortingBoundedInputBenchmarks extends BenchmarkBase {

	private static final int RECORDS_PER_INVOCATION = 1_500_000;
	private static final List<Integer> INDICES = IntStream.range(0, 10).boxed().collect(Collectors.toList());
	static {
		Collections.shuffle(INDICES);
	}

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + SortingBoundedInputBenchmarks.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@State(Thread)
	public static class SortingInputContext extends FlinkEnvironmentContext {
		@Override
		protected Configuration createConfiguration() {
			Configuration configuration = super.createConfiguration();
			configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
			configuration.set(AlgorithmOptions.SORT_SPILLING_THRESHOLD, 0f);
			return configuration;
		}
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void sortedOneInput(SortingInputContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;

		DataStreamSource<Integer> elements = env.fromParallelCollection(
			new InputGenerator(RECORDS_PER_INVOCATION),
			BasicTypeInfo.INT_TYPE_INFO
		);

		SingleOutputStreamOperator<Long> counts = elements
			.keyBy(element -> element)
			.transform(
				"Asserting operator",
				BasicTypeInfo.LONG_TYPE_INFO,
				new AssertingOperator()
			);

		counts.addSink(new DiscardingSink<>());
		context.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void sortedTwoInput(SortingInputContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;

		DataStreamSource<Integer> elements1 = env.fromParallelCollection(
			new InputGenerator(RECORDS_PER_INVOCATION / 2),
			BasicTypeInfo.INT_TYPE_INFO
		);

		DataStreamSource<Integer> elements2 = env.fromParallelCollection(
			new InputGenerator(RECORDS_PER_INVOCATION / 2),
			BasicTypeInfo.INT_TYPE_INFO
		);
		SingleOutputStreamOperator<Long> counts = elements1.connect(elements2)
			.keyBy(
				element -> element,
				element -> element
			)
			.transform(
				"Asserting operator",
				BasicTypeInfo.LONG_TYPE_INFO,
				new AssertingTwoInputOperator()
			);

		counts.addSink(new DiscardingSink<>());
		context.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void sortedMultiInput(SortingInputContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;

		KeyedStream<Integer, Object> elements1 = env.fromParallelCollection(
			new InputGenerator(RECORDS_PER_INVOCATION / 3),
			BasicTypeInfo.INT_TYPE_INFO
		).keyBy(el -> el);

		KeyedStream<Integer, Object> elements2 = env.fromParallelCollection(
			new InputGenerator(RECORDS_PER_INVOCATION / 3),
			BasicTypeInfo.INT_TYPE_INFO
		).keyBy(el -> el);

		KeyedStream<Integer, Object> elements3 = env.fromParallelCollection(
			new InputGenerator(RECORDS_PER_INVOCATION / 3),
			BasicTypeInfo.INT_TYPE_INFO
		).keyBy(el -> el);

		KeyedMultipleInputTransformation<Long> assertingTransformation = new KeyedMultipleInputTransformation<>(
			"Asserting operator",
			new AssertingThreeInputOperatorFactory(),
			BasicTypeInfo.LONG_TYPE_INFO,
			-1,
			BasicTypeInfo.INT_TYPE_INFO
		);
		assertingTransformation.addInput(elements1.getTransformation(), elements1.getKeySelector());
		assertingTransformation.addInput(elements2.getTransformation(), elements2.getKeySelector());
		assertingTransformation.addInput(elements3.getTransformation(), elements3.getKeySelector());

		env.addOperator(assertingTransformation);
		DataStream<Long> counts = new DataStream<>(env, assertingTransformation);

		counts.addSink(new DiscardingSink<>());
		context.execute();
	}

	private static final class ProcessedKeysOrderAsserter implements Serializable {
		private final Set<Integer> seenKeys = new HashSet<>();
		private long seenRecords = 0;
		private Integer currentKey = null;

		public void processElement(Integer element) {
			this.seenRecords++;
			if (!Objects.equals(element, currentKey)) {
				if (!seenKeys.add(element)) {
					Assert.fail("Received an out of order key: " + element);
				}
				currentKey = element;
			}
		}

		public long getSeenRecords() {
			return seenRecords;
		}
	}

	private static class AssertingOperator extends AbstractStreamOperator<Long>
			implements OneInputStreamOperator<Integer, Long>, BoundedOneInput {
		private final ProcessedKeysOrderAsserter asserter = new ProcessedKeysOrderAsserter();

		@Override
		public void processElement(StreamRecord<Integer> element) {
			asserter.processElement(element.getValue());
		}

		@Override
		public void endInput() {
			output.collect(new StreamRecord<>(asserter.getSeenRecords()));
		}

	}

	private static class AssertingTwoInputOperator extends AbstractStreamOperator<Long>
			implements TwoInputStreamOperator<Integer, Integer, Long>, BoundedMultiInput {
		private final ProcessedKeysOrderAsserter asserter = new ProcessedKeysOrderAsserter();
		private boolean input1Finished = false;
		private boolean input2Finished = false;

		@Override
		public void processElement1(StreamRecord<Integer> element) {
			asserter.processElement(element.getValue());
		}

		@Override
		public void processElement2(StreamRecord<Integer> element) {
			asserter.processElement(element.getValue());
		}

		@Override
		public void endInput(int inputId) {
			if (inputId == 1) {
				input1Finished = true;
			}

			if (inputId == 2) {
				input2Finished = true;
			}

			if (input1Finished && input2Finished) {
				output.collect(new StreamRecord<>(asserter.getSeenRecords()));
			}
		}
	}

	private static class AssertingThreeInputOperator extends AbstractStreamOperatorV2<Long>
			implements MultipleInputStreamOperator<Long>, BoundedMultiInput {
		private final ProcessedKeysOrderAsserter asserter = new ProcessedKeysOrderAsserter();
		private boolean input1Finished = false;
		private boolean input2Finished = false;
		private boolean input3Finished = false;

		public AssertingThreeInputOperator(
				StreamOperatorParameters<Long> parameters,
				int numberOfInputs) {
			super(parameters, 3);
			assert numberOfInputs == 3;
		}

		@Override
		public void endInput(int inputId) {
			if (inputId == 1) {
				input1Finished = true;
			}

			if (inputId == 2) {
				input2Finished = true;
			}

			if (inputId == 3) {
				input3Finished = true;
			}

			if (input1Finished && input2Finished && input3Finished) {
				output.collect(new StreamRecord<>(asserter.getSeenRecords()));
			}
		}

		@Override
		public List<Input> getInputs() {
			return Arrays.asList(
				new SingleInput(asserter::processElement),
				new SingleInput(asserter::processElement),
				new SingleInput(asserter::processElement)
			);
		}
	}

	private static class AssertingThreeInputOperatorFactory implements StreamOperatorFactory<Long> {
		@Override
		@SuppressWarnings("unchecked")
		public <T extends StreamOperator<Long>> T createStreamOperator(StreamOperatorParameters<Long> parameters) {
			return (T) new AssertingThreeInputOperator(parameters, 3);
		}

		@Override
		public void setChainingStrategy(ChainingStrategy strategy) {

		}

		@Override
		public ChainingStrategy getChainingStrategy() {
			return ChainingStrategy.NEVER;
		}

		@Override
		public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
			return AssertingThreeInputOperator.class;
		}
	}

	private static class SingleInput implements Input<Integer> {

		private final Consumer<Integer> recordConsumer;

		private SingleInput(Consumer<Integer> recordConsumer) {
			this.recordConsumer = recordConsumer;
		}

		@Override
		public void processElement(StreamRecord<Integer> element) {
			recordConsumer.accept(element.getValue());
		}

		@Override
		public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) {

		}

		@Override
		public void processLatencyMarker(LatencyMarker latencyMarker) {

		}

		@Override
		public void setKeyContextElement(StreamRecord<Integer> record) {

		}
	}

	private static class InputGenerator extends SplittableIterator<Integer> {

		private final long numberOfRecords;
		private long generatedRecords;

		private InputGenerator(long numberOfRecords) {
			this.numberOfRecords = numberOfRecords;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Iterator<Integer>[] split(int numPartitions) {
			long numberOfRecordsPerPartition = numberOfRecords / numPartitions;
			long remainder = numberOfRecords % numPartitions;
			Iterator<Integer>[] iterators = new Iterator[numPartitions];

			for (int i = 0; i < numPartitions - 1; i++) {
				iterators[i] = new InputGenerator(numberOfRecordsPerPartition);
			}

			iterators[numPartitions - 1] = new InputGenerator(numberOfRecordsPerPartition + remainder);

			return iterators;
		}

		@Override
		public int getMaximumNumberOfSplits() {
			return (int) Math.min(numberOfRecords, Integer.MAX_VALUE);
		}

		@Override
		public boolean hasNext() {
			return generatedRecords < numberOfRecords;
		}

		@Override
		public Integer next() {
			if (hasNext()) {
				generatedRecords++;
				return INDICES.get((int) (generatedRecords % INDICES.size()));
			}

			return null;
		}
	}
}
