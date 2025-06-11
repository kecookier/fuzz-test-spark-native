#!/bin/bash

export SPARK_HOME=/opt/meituan/zhaokuo03/dev/spark-3.5
SPARK_MASTER=local
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --class org.apache.gluten.fuzz.SparkFunctionAnalyzer \
    target/fuzz-test-spark-native-1.0-SNAPSHOT.jar

#scala -cp target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q) org.apache.gluten.fuzz.SparkFunctionAnalyzer
