name := "myKafkaStreams"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-core" % "2.14.1",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.14.1" % Test,
  "org.apache.kafka" %% "kafka" % "2.8.0",
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.kafka" % "kafka-streams" % "2.8.0" % Test,
  "org.apache.camel.kafkaconnector" % "camel-kafka-connector" % "0.10.1",
  "org.apache.kafka" % "connect-api" % "2.8.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0",
  "com.google.code.gson" % "gson" % "2.8.7",
  "org.slf4j" % "slf4j-log4j12" % "2.0.0-alpha1" ,
  "org.slf4j" % "slf4j-simple" % "2.0.0-alpha1",
  "org.slf4j" % "slf4j-api" % "2.0.0-alpha1",
  "com.github.javafaker" % "javafaker" % "1.0.2",
  "org.apache.kafka" % "kafka-streams-test-utils" % "2.8.0" % Test,
  "org.hamcrest" % "hamcrest" % "2.2" % Test,
  "org.mockito" % "mockito-core" % "3.11.2" % Test,
  "org.mockito" % "mockito-junit-jupiter" % "3.11.2" % Test

)