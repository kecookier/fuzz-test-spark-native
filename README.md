# Spark Native Engine Fuzz Testing

This is a fuzz testing framework for the Spark Native Engine. It is mostly copied from [datafusion-comet/fuzz-testing](https://github.com/apache/datafusion-comet/tree/main/fuzz-testing).

I use it to test the popular spark native engines:
- [Datafusion Comet](https://github.com/apache/datafusion-comet)
- [Blaze](https://github.com/kwai/blaze/)
- [Gluten](https://github.com/apache/incubator-gluten)

There is a scheduled [Spark Native Engine Fuzz Testing GHA](https://github.com/wForget/fuzz-test-spark-native/actions/workflows/master.yml) that runs fuzz tests after building for each native engine, and reports the test results in issues.

You can see the fuzz test reports of each native engine in [issues list](https://github.com/wForget/fuzz-test-spark-native/issues), which shows the running failures and consistency issues in spark with native engine.

**Note**: The report may be truncated due to issue comment length limit. You can download the complete report file in the action's artifacts.

**Bugs found by fuzz testing**: https://github.com/wForget/fuzz-test-spark-native/issues/7


usage:
scala -cp target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q) org.apache.gluten.fuzz.SparkFunctionAnalyzer
scala -cp target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q) org.apache.gluten.fuzz.SparkFunctionAnalyzer --detail
