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

/*
 * If you want to create updated Java Thrift classes, you need to install the
 * thrift binary with a matching version to the dependency inside the pom.xml or
 * override it and generate classes via
 *
 * > mvn generate-sources -Pgenerate-thrift -Dthrift.version=0.13.0
 *
 * or more dynamically:
 *
 * > mvn generate-sources -Pgenerate-thrift -Dthrift.version=$(thrift --version | rev | cut -d' ' -f 1 | rev)
 *
 * Be sure to use the same thrift version when compiling and running
 * tests/benchmarks as well to avoid potential conflicts.
 */

namespace java org.apache.flink.benchmark.thrift

    typedef i32 int

    struct MyPojo {
        1: int id;
        2: string name;
        3: list<string> operationName;
        4: list<MyOperation> operations;
        5: int otherId1;
        6: int otherId2;
        7: int otherId3;
        8: optional string someObject;
    }

    struct MyOperation {
        1: int id;
        2: string name;
    }
