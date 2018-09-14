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
