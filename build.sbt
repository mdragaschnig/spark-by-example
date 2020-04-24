name := "spark-by-example"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.2.0"
val hadoopVersion = "2.6.5"
val sparksortedVersion = "1.2.0"

resolvers in Global += Resolver.mavenLocal
//resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
//resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
// Scala 2.11
//libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11"
libraryDependencies += "com.vividsolutions" % "jts-core" % "1.14.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion%"provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies += "com.tresata" %% "spark-sorted" % "1.2.0"
//libraryDependencies += "org.gavaghan" % "geodesy" % "1.1.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

