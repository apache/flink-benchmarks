#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

JAVA_ARGS=()
JMH_ARGS=()
BINARY="java"
BENCHMARK_PATTERN=

while getopts ":j:c:b:e:p:a:m:h" opt; do
  case $opt in
    j) JAVA_ARGS+=("${OPTARG}")
    ;;
    c) CLASSPATH_ARG="${OPTARG}"
    ;;
    b) BINARY="${OPTARG}"
    ;;
    p) PROFILER_ARG="${OPTARG:+-prof ${OPTARG}}"
    # conditional prefixing inspired by https://stackoverflow.com/a/40771884/1389220
    ;;
    a) JMH_ARGS+=("${OPTARG}")
    ;;
    e) BENCHMARK_EXCLUDES="${OPTARG:+-e ${OPTARG}}"
    ;;
    m) BENCHMARK_PATTERN="${OPTARG}"
    ;;
    h)
      1>&2 cat << EOF
usage: $0 -c ${CLASSPATH} [-j JVM_ARG] [-b /path/to/java] [-a JMH_ARG] [-p <profiler>:<opt1=X;opt2=Y] [-e benchmark exclusions]
-j JVM argument. Can be used 0 - n times
-c the classpath to use for JMH
-b path to the java binary to use for the benchmark run
-p Profiler argument. Accepts the value to be passed to jmh -prof option
-a additional JMH command line argument. Can be used 0 - N times
-e the regex for JMH to exclude benchmarks.
-h this help message
EOF
      exit 1
    ;;
    \?) echo "Invalid option -$opt ${OPTARG}" >&2
    exit 1
    ;;
  esac
done
shift "$(($OPTIND -1))"

# shellcheck disable=SC2086
${BINARY} "${JAVA_ARGS[@]}" \
  -classpath "${CLASSPATH_ARG}" \
   org.openjdk.jmh.Main \
   -foe true \
   -rf csv \
   "${JMH_ARGS[@]}" \
   ${PROFILER_ARG:-} \
   ${BENCHMARK_EXCLUDES:-} \
   "${BENCHMARK_PATTERN:-.*}"
