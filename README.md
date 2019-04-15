# flink-benchmarks

This repository contains sets of micro benchmarks designed to run on single machine to help 
[Apache Flink's](https://github.com/apache/flink) developers assess performance implications of 
their changes. 

The main methods defined in the various classes (test cases) are using [jmh](http://openjdk.java.net/projects/code-tools/jmh/)  micro
benchmark suite to define runners to execute those test cases. You can either execute 
whole benchmark suite (which takes ~1hour) at once:

```
mvn -Dflink.version=1.5.0 clean install exec:exec
```

or if you want to execute just one benchmark, the best approach is to execute selected main function manually. 
For example from your IDE (hint there is a plugin for Intellij IDEA). In that case don't forget about selecting 
`flink.version`, default value for the property is defined in pom.xml.

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
