name := "vehicle-metrics-processor"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test
libraryDependencies += "org.mockito" %% "mockito-scala-scalatest" % "1.16.0" % Test
libraryDependencies += "org.mockito" % "mockito-inline" % "3.5.13" % Test



