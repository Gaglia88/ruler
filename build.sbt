name := "RulER"
version := "1.0"
scalaVersion := "2.11.8"

//unmanagedBase := baseDirectory.value / "custom_lib"

// https://mvnrepository.com/artifact/commons-codec/commons-codec
//libraryDependencies += "commons-codec" % "commons-codec" % "1.11"

// https://mvnrepository.com/artifact/org.json/json
libraryDependencies += "org.json" % "json" % "20170516"

// https://mvnrepository.com/artifact/org.apache.commons/commons-csv
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.6"

// https://mvnrepository.com/artifact/com.google.guava/guava
//libraryDependencies += "com.google.guava" % "guava" % "26.0-jre"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"

mainClass in Compile := Some("Experiments.Test")