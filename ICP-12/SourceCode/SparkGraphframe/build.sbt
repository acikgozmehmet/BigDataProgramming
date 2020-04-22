name := "SparkDataframe"
scalaVersion := "2.11.12"

version := "0.1"

scalaVersion := "2.11.12"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.5"
// https://mvnrepository.com/artifact/com.databricks/spark-csv
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.5"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.3-s_2.11"