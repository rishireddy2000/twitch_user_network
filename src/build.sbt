name := "twitch-network-processor"
version := "1.0"
scalaVersion := "2.12.15"

scalaSource in Compile := baseDirectory.value

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.apache.spark" %% "spark-streaming" % "3.3.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.1",
  "org.apache.spark" %% "spark-hive" % "3.3.1",
  "org.apache.kafka" % "kafka-clients" % "2.8.1"
)

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case "reference.conf"             => MergeStrategy.concat
  case x => MergeStrategy.first
}
