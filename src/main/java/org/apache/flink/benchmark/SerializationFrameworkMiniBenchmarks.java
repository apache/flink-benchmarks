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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.benchmark.full.StringSerializationBenchmark;
import org.apache.flink.benchmark.functions.BaseSourceWithKeyRange;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.types.Row;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Benchmark for serializing POJOs and Tuples with different serialization frameworks.
 */
public class SerializationFrameworkMiniBenchmarks extends BenchmarkBase {

	protected static final int RECORDS_PER_INVOCATION = 300_000;

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + SerializationFrameworkMiniBenchmarks.class.getSimpleName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerPojo(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.registerPojoType(MyPojo.class);
		executionConfig.registerPojoType(MyOperation.class);

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerHeavyString(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(1);
		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.registerPojoType(MyPojo.class);
		executionConfig.registerPojoType(MyOperation.class);

		env.addSource(new LongStringSource(RECORDS_PER_INVOCATION, 12))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerTuple(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);

		env.addSource(new TupleSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerKryo(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.enableForceKryo();
		executionConfig.registerKryoType(MyPojo.class);
		executionConfig.registerKryoType(MyOperation.class);

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerAvro(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);

		env.addSource(new AvroPojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerRow(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);

		env.addSource(new RowSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	/**
	 * Source emitting a long String.
	 */
	public static class LongStringSource extends BaseSourceWithKeyRange<String> {
		private static final long serialVersionUID = 3746240885982877398L;
		private String[] templates;

		public LongStringSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		protected void init() {
			super.init();
			templates = new String[] {
					makeString(StringSerializationBenchmark.asciiChars, 1024),
					makeString(StringSerializationBenchmark.russianChars, 1024),
					makeString(StringSerializationBenchmark.chineseChars, 1024)
			};
		}

		private String makeString(char[] symbols, int length) {
			char[] buffer = new char[length];
			Random random = ThreadLocalRandom.current();
			Arrays.fill(buffer, symbols[random.nextInt(symbols.length)]);
			return new String(buffer);
		}

		@Override
		protected String getElement(int keyId) {
			return templates[keyId % templates.length];
		}
	}

	/**
	 * Source emitting a simple {@link MyPojo POJO}.
	 */
	public static class PojoSource extends BaseSourceWithKeyRange<MyPojo> {
		private static final long serialVersionUID = 2941333602938145526L;

		private transient MyPojo template;

		public PojoSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		protected void init() {
			super.init();
			template = new MyPojo(
					0,
					"myName",
					new String[] {"op1", "op2", "op3", "op4"},
					new MyOperation[] {
							new MyOperation(1, "op1"),
							new MyOperation(2, "op2"),
							new MyOperation(3, "op3")},
					1,
					2,
					3,
					"null");
		}

		@Override
		protected MyPojo getElement(int keyId) {
			template.setId(keyId);
			return template;
		}
	}

	/**
	 * Source emitting a {@link org.apache.flink.benchmark.avro.MyPojo POJO} generated by an Avro schema.
	 */
	public static class AvroPojoSource extends BaseSourceWithKeyRange<org.apache.flink.benchmark.avro.MyPojo> {
		private static final long serialVersionUID = 2941333602938145526L;

		private transient org.apache.flink.benchmark.avro.MyPojo template;

		public AvroPojoSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		protected void init() {
			super.init();
			template = new org.apache.flink.benchmark.avro.MyPojo(
					0,
					"myName",
					Arrays.asList("op1", "op2", "op3", "op4"),
					Arrays.asList(
							new org.apache.flink.benchmark.avro.MyOperation(1, "op1"),
							new org.apache.flink.benchmark.avro.MyOperation(2, "op2"),
							new org.apache.flink.benchmark.avro.MyOperation(3, "op3")),
					1,
					2,
					3,
					"null");
		}

		@Override
		protected org.apache.flink.benchmark.avro.MyPojo getElement(int keyId) {
			template.setId(keyId);
			return template;
		}
	}

	/**
	 * Source emitting a <tt>Tuple</tt> based on {@link MyPojo}.
	 */
	public static class TupleSource extends BaseSourceWithKeyRange<Tuple8<Integer, String, String[], Tuple2<Integer, String>[], Integer, Integer, Integer, Object>> {
		private static final long serialVersionUID = 2941333602938145526L;

		private transient Tuple8 template;

		public TupleSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void init() {
			super.init();
			template = MyPojo.createTuple(
					0,
					"myName",
					new String[] {"op1", "op2", "op3", "op4"},
					new Tuple2[] {
							MyOperation.createTuple(1, "op1"),
							MyOperation.createTuple(2, "op2"),
							MyOperation.createTuple(3, "op3")},
					1,
					2,
					3,
					"null");
		}

		@Override
		protected Tuple8<Integer, String, String[], Tuple2<Integer, String>[], Integer, Integer, Integer, Object> getElement(int keyId) {
			template.setField(keyId, 0);
			return template;
		}
	}

	/**
	 * Source emitting a {@link Row} based on {@link MyPojo}.
	 */
	public static class RowSource extends BaseSourceWithKeyRange<Row> implements ResultTypeQueryable<Row> {
		private static final long serialVersionUID = 2941333602938145526L;

		private transient Row template;

		public RowSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void init() {
			super.init();
			template = MyPojo.createRow(
					0,
					"myName",
					new String[] {"op1", "op2", "op3", "op4"},
					new Row[] {
							MyOperation.createRow(1, "op1"),
							MyOperation.createRow(2, "op2"),
							MyOperation.createRow(3, "op3")},
					1,
					2,
					3,
					"null");
		}

		@Override
		protected Row getElement(int keyId) {
			template.setField(0, keyId);
			return template;
		}

		@Override
		public TypeInformation<Row> getProducedType() {
			return MyPojo.getProducedRowType();
		}
	}

	/**
	 * Not so simple POJO.
	 */
	@SuppressWarnings({"WeakerAccess", "unused"})
	public static class MyPojo {
		public int id;
		private String name;
		private String[] operationNames;
		private MyOperation[] operations;
		private int otherId1;
		private int otherId2;
		private int otherId3;
		private Object someObject;

		public MyPojo() {
		}

		public MyPojo(
				int id,
				String name,
				String[] operationNames,
				MyOperation[] operations,
				int otherId1,
				int otherId2,
				int otherId3,
				Object someObject) {
			this.id = id;
			this.name = name;
			this.operationNames = operationNames;
			this.operations = operations;
			this.otherId1 = otherId1;
			this.otherId2 = otherId2;
			this.otherId3 = otherId3;
			this.someObject = someObject;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String[] getOperationNames() {
			return operationNames;
		}

		public void setOperationNames(String[] operationNames) {
			this.operationNames = operationNames;
		}

		public MyOperation[] getOperations() {
			return operations;
		}

		public void setOperations(
				MyOperation[] operations) {
			this.operations = operations;
		}

		public int getOtherId1() {
			return otherId1;
		}

		public void setOtherId1(int otherId1) {
			this.otherId1 = otherId1;
		}

		public int getOtherId2() {
			return otherId2;
		}

		public void setOtherId2(int otherId2) {
			this.otherId2 = otherId2;
		}

		public int getOtherId3() {
			return otherId3;
		}

		public void setOtherId3(int otherId3) {
			this.otherId3 = otherId3;
		}

		public Object getSomeObject() {
			return someObject;
		}

		public void setSomeObject(Object someObject) {
			this.someObject = someObject;
		}

		public static Tuple8<Integer, String, String[], Tuple2<Integer, String>[], Integer, Integer, Integer, Object> createTuple(
				int id,
				String name,
				String[] operationNames,
				Tuple2<Integer, String>[] operations,
				int otherId1,
				int otherId2,
				int otherId3,
				Object someObject) {
			return Tuple8.of(id, name, operationNames, operations, otherId1, otherId2, otherId3, someObject);
		}

		public static Row createRow(
				int id,
				String name,
				String[] operationNames,
				Row[] operations,
				int otherId1,
				int otherId2,
				int otherId3,
				Object someObject) {
			return Row.of(id, name, operationNames, operations, otherId1, otherId2, otherId3, someObject);
		}

		public static TypeInformation<Row> getProducedRowType() {
			return Types.ROW(
					Types.INT,
					Types.STRING,
					Types.OBJECT_ARRAY(Types.STRING),
					Types.OBJECT_ARRAY(Types.ROW(Types.INT, Types.STRING)),
					Types.INT,
					Types.INT,
					Types.INT,
					Types.GENERIC(Object.class)
			);
		}
	}

	/**
	 * Another POJO.
	 */
	@SuppressWarnings({"WeakerAccess", "unused"})
	public static class MyOperation {
		int id;
		protected String name;

		public MyOperation() {
		}

		public MyOperation(int id, String name) {
			this.id = id;
			this.name = name;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public static Tuple2<Integer, String> createTuple(int id, String name) {
			return Tuple2.of(id, name);
		}

		public static Row createRow(int id, String name) {
			return Row.of(id, name);
		}
	}
}
