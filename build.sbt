name := "kafka-to-cassandra"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= {
  val sparkV = "2.2.0"
  val cassandraV = "2.0.5"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkV,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "org.apache.spark" %% "spark-hive" % sparkV,
    "com.datastax.spark" %% "spark-cassandra-connector" % cassandraV,
    "org.scalatest" %% "scalatest" % "3.0.3" % "test",
    "com.github.nscala-time" %% "nscala-time" % "2.12.0",
    "com.typesafe" % "config" % "1.3.1",
    "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.2",
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0"
  )
}

mainClass in (Compile, run) :=  Some("com.kafkaToSparkToCass.Main")

mainClass in assembly := Some("com.kafkaToSparkToCass.Main")

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") =>
    MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
