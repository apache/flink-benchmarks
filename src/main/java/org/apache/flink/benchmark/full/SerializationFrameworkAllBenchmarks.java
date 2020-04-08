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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.benchmark.FlinkEnvironmentContext;
import org.apache.flink.benchmark.SerializationFrameworkMiniBenchmarks;
import org.apache.flink.benchmark.functions.BaseSourceWithKeyRange;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import com.twitter.chill.protobuf.ProtobufSerializer;
import com.twitter.chill.thrift.TBaseSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Benchmark for serializing POJOs and Tuples with different serialization frameworks.
 */
public class SerializationFrameworkAllBenchmarks extends SerializationFrameworkMiniBenchmarks {

	public static void main(String[] args) throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + SerializationFrameworkAllBenchmarks.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerPojoWithoutRegistration(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerKryoWithoutRegistration(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		env.getConfig().enableForceKryo();

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerAvroReflect(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		env.getConfig().enableForceAvro();

		env.addSource(new PojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
	public void serializerAvroGeneric(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);

		Schema schema = AvroGenericRecordSource.loadSchema();
		env.addSource(new AvroGenericRecordSource(RECORDS_PER_INVOCATION, 10, schema))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerKryoThrift(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.enableForceKryo();
		executionConfig.addDefaultKryoSerializer(org.apache.flink.benchmark.thrift.MyPojo.class, TBaseSerializer.class);
		executionConfig.addDefaultKryoSerializer(org.apache.flink.benchmark.thrift.MyOperation.class, TBaseSerializer.class);

		env.addSource(new ThriftPojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	@Benchmark
	@OperationsPerInvocation(value = SerializationFrameworkMiniBenchmarks.RECORDS_PER_INVOCATION)
	public void serializerKryoProtobuf(FlinkEnvironmentContext context) throws Exception {
		StreamExecutionEnvironment env = context.env;
		env.setParallelism(4);
		ExecutionConfig executionConfig = env.getConfig();
		executionConfig.enableForceKryo();
		executionConfig.registerTypeWithKryoSerializer(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo.class, ProtobufSerializer.class);
		executionConfig.registerTypeWithKryoSerializer(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyOperation.class, ProtobufSerializer.class);

		env.addSource(new ProtobufPojoSource(RECORDS_PER_INVOCATION, 10))
				.rebalance()
				.addSink(new DiscardingSink<>());

		env.execute();
	}

	/**
	 * Source emitting an Avro GenericRecord.
	 */
	public static class AvroGenericRecordSource extends BaseSourceWithKeyRange<GenericRecord> implements
			ResultTypeQueryable<GenericRecord> {
		private static final long serialVersionUID = 2941333602938145526L;

		private final GenericRecordAvroTypeInfo producedType;
		private transient Schema myPojoSchema;
		private final String schemaString;

		private transient GenericRecord template;

		public AvroGenericRecordSource(int numEvents, int numKeys, Schema schema) {
			super(numEvents, numKeys);
			this.producedType = new GenericRecordAvroTypeInfo(schema);
			this.myPojoSchema = schema;
			this.schemaString = schema.toString();
		}

		private static Schema loadSchema() throws IOException {
			ClassLoader classLoader = ClassLoader.getSystemClassLoader();
			try (InputStream is = classLoader.getResourceAsStream("avro/mypojo.avsc")) {
				if (is == null) {
					throw new FileNotFoundException("File 'mypojo.avsc' not found");
				}
				return new Schema.Parser().parse(is);
			}
		}

		@Override
		protected void init() {
			super.init();

			if (myPojoSchema == null) {
				this.myPojoSchema = new Schema.Parser().parse(schemaString);
			}
			Schema myOperationSchema = myPojoSchema.getField("operations").schema().getElementType();

			template = new GenericData.Record(myPojoSchema);
			template.put("id", 0);
			template.put("name", "myName");
			template.put("operationName", Arrays.asList("op1", "op2", "op3", "op4"));

			GenericData.Record op1 = new GenericData.Record(myOperationSchema);
			op1.put("id", 1);
			op1.put("name", "op1");
			GenericData.Record op2 = new GenericData.Record(myOperationSchema);
			op2.put("id", 2);
			op2.put("name", "op2");
			GenericData.Record op3 = new GenericData.Record(myOperationSchema);
			op3.put("id", 3);
			op3.put("name", "op3");
			template.put("operations", Arrays.asList(op1, op2, op3));

			template.put("otherId1", 1);
			template.put("otherId2", 2);
			template.put("otherId3", 3);
			template.put("nullable", "null");
		}

		@Override
		protected GenericRecord getElement(int keyId) {
			template.put("id", keyId);
			return template;
		}

		@Override
		public TypeInformation<GenericRecord> getProducedType() {
			return producedType;
		}
	}

	/**
	 * Source emitting a {@link org.apache.flink.benchmark.thrift.MyPojo POJO} generated by an Apache Thrift schema.
	 */
	public static class ThriftPojoSource extends BaseSourceWithKeyRange<org.apache.flink.benchmark.thrift.MyPojo> {
		private static final long serialVersionUID = 2941333602938145526L;

		private transient org.apache.flink.benchmark.thrift.MyPojo template;

		public ThriftPojoSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		protected void init() {
			super.init();
			template = new org.apache.flink.benchmark.thrift.MyPojo(
					0,
					"myName",
					Arrays.asList("op1", "op2", "op3", "op4"),
					Arrays.asList(
							new org.apache.flink.benchmark.thrift.MyOperation(1, "op1"),
							new org.apache.flink.benchmark.thrift.MyOperation(2, "op2"),
							new org.apache.flink.benchmark.thrift.MyOperation(3, "op3")),
					1,
					2,
					3);
			template.setSomeObject("null");
		}

		@Override
		protected org.apache.flink.benchmark.thrift.MyPojo getElement(int keyId) {
			template.setId(keyId);
			return template;
		}
	}

	/**
	 * Source emitting a {@link org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo POJO} generated by a Protobuf schema.
	 */
	public static class ProtobufPojoSource extends BaseSourceWithKeyRange<org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo> {
		private static final long serialVersionUID = 2941333602938145526L;

			private transient org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo template;

		public ProtobufPojoSource(int numEvents, int numKeys) {
			super(numEvents, numKeys);
		}

		@Override
		protected void init() {
			super.init();
			template = org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo.newBuilder()
					.setId(0)
					.setName("myName")
					.addAllOperationName(Arrays.asList("op1", "op2", "op3", "op4"))
					.addOperations(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyOperation.newBuilder()
							.setId(1)
							.setName("op1"))
					.addOperations(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyOperation.newBuilder()
							.setId(2)
							.setName("op2"))
					.addOperations(org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyOperation.newBuilder()
							.setId(3)
							.setName("op3"))
					.setOtherId1(1)
					.setOtherId2(2)
					.setOtherId3(3)
					.setSomeObject("null")
					.build();
		}

		@Override
		protected org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo getElement(int keyId) {
			return org.apache.flink.benchmark.protobuf.MyPojoOuterClass.MyPojo.newBuilder(template)
					.setId(keyId)
					.build();
		}
	}
}
