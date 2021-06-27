name := "myKafkaStreams"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-core" % "2.14.1",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.14.1" % Test,
  "org.apache.kafka" %% "kafka" % "2.8.0",
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.kafka" % "kafka-streams" % "2.8.0",
  "org.apache.camel.kafkaconnector" % "camel-kafka-connector" % "0.10.1",
  "org.apache.kafka" % "connect-api" % "2.8.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0",
  "com.google.code.gson" % "gson" % "2.8.7"
)