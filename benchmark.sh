#!/usr/bin/env bash

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
usage: $0 -c ${CLASSPATH} [-j JVM_ARG]* [-b /path/to/java] [-a JMH_ARG]* [-p <profiler>:<opt1=X;opt2=Y] [-e benchmark exclusions]
-j JVM argument. can be used 0 - n times
-c the classpath to use for JMH
-b path to the java binary to use for the benchmark run
-p Profiler argument. Accepts the value to be passed to jmh -prof option
-a additional JMH command line argument.
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
