scala -cp target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q) org.apache.gluten.fuzz.SparkFunctionAnalyzer
