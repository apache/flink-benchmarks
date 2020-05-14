# flink-benchmarks

This repository contains sets of micro benchmarks designed to run on single machine to help 
[Apache Flink's](https://github.com/apache/flink) developers assess performance implications of 
their changes. 

The main methods defined in the various classes (test cases) are using [jmh](http://openjdk.java.net/projects/code-tools/jmh/)  micro
benchmark suite to define runners to execute those test cases. You can execute the
default benchmark suite (which takes ~1hour) at once:

```
mvn -Dflink.version=1.5.0 clean install exec:exec
```

There is also a separate benchmark suit for state backend, and you can execute this suit (which takes ~1hour) using
below command:

```
mvn -Dflink.version=1.5.0 clean package exec:exec \
 -Dexec.executable=java -Dexec.args="-jar target/benchmarks.jar -rf csv org.apache.flink.state.benchmark.*"
```

If you want to execute just one benchmark, the best approach is to execute selected main function manually.
There're mainly two ways:

1. From your IDE (hint there is a plugin for Intellij IDEA).
   * In this case don't forget about selecting `flink.version`, default value for the property is defined in pom.xml.

2. From command line, using command like:
   ```
   mvn -Dflink.version=1.5.0 clean package exec:exec \
    -Dexec.executable=java -Dexec.args="-jar target/benchmarks.jar <benchmark_class>"
   ```

We also support to run each benchmark once (with only one fork and one iteration) for testing, with below command:

```
mvn test -P test
```

## Prerequisites

The recent addition of OpenSSL-based benchmarks require one of two modes to be active:
- dynamically-linked OpenSSL (default): this uses a dependency with native wrappers that are linked dynamically to your system's libraries. Depending on the installed OpenSSL version and OS, this may fail and you should try the next option:
- statically-linked OpenSSL: this can be activated by `mvn -Dnetty-tcnative.flavor=static` but requires `flink-shaded-netty-tcnative-static` in the version from `pom.xml`. This module is not provided by Apache Flink by default due to licensing issues (see https://issues.apache.org/jira/browse/LEGAL-393) but can be generated from inside a corresponding `flink-shaded` source via:
```
mvn clean install -Pinclude-netty-tcnative-static -pl flink-shaded-netty-tcnative-static
```

If both options are not working, OpenSSL benchmarks will fail but that should not influence any other benchmarks.

## Code structure

Recommended code structure is to define all benchmarks in [Apache Flink](https://github.com/apache/flink)
and only wrap them here, in this repository, into executor classes. 

Such code structured is due to using GPL2 licensed [jmh](http://openjdk.java.net/projects/code-tools/jmh/) library
for the actual execution of the benchmarks. Ideally we would prefer to have all of the code moved to [Apache Flink](https://github.com/apache/flink)

## General remarks

- If you can not measure the performance difference, then just don't bother (avoid premature optimisations).
- Make sure that you are not disturbing the benchmarks. While benchmarking, you shouldn't be touching the machine that's running the benchmarks. Scrolling web page in a browser or changing windows (alt/cmd + tab) can seriously affect the results.
- If in doubt, verify the results more then once, like:
  1. measure the base line
  2. measure the change, for example +5% performance improvement
  3. switch back to the base line, make sure that the result is worse
  4. go back to the change and verify +5% performance improvement
  5. if something doesn't show the results that you were expecting, investigate and don't ignore this! Maybe there is some larger performance instability and your previous results were just lucky/unlucky flukes.
- Some results can show up over the benchmarking noise only in long term trends.
- Please tune the length of the benchmark (usually by number of processed records). The less records, the faster the benchmark, the more iterations can be executed, however the higher chance of one of setup overheads skewing the results. Rule of thumb is that you should increase the number of processed records up to a point where results stop improving visibly, while trying to keep the single benchmark execution under 1 second.

### Naming convention

Regarding naming the benchmark methods, there is one important thing.
When uploading the results to the [codespeed web UI](http://codespeed.dak8s.net:8000),
uploader is using just the benchmark's method name combined with the parameters
to generate visible name of the benchmark in the UI. Because of that it is important to:
1. Have the method name explanatory enough so we can quickly recognize what the given benchmark is all about, using just the benchmark's method name.
2. Be as short as possible to not bloat the web UI (so we must drop all of the redundant suffixes/prefixes, like `benchmark`, `test`, ...)
3. Class name is completely ignored by the uploader.
4. Method names should be unique across the project.

Good example of how to name benchmark methods are:
- `networkThroughput`
- `sessionWindow`

### Submitting a pull request

Please attach the results of your benchmarks.
