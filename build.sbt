name := "myKafkaStreams"

version := "0.1"

scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0",
  "org.apache.kafka" %% "kafka" % "3.4.0",
  "org.apache.kafka" % "kafka-clients" % "3.4.0",
  "org.apache.kafka" % "kafka-streams" % "3.4.0",
  "org.apache.camel.kafkaconnector" % "camel-kafka-connector" % "0.10.1",
  "org.apache.kafka" % "connect-api" % "3.4.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.4.0",
  "com.google.code.gson" % "gson" % "2.10.1",
  "org.slf4j" % "slf4j-log4j12" % "2.0.7" ,
  "org.slf4j" % "slf4j-simple" % "2.0.7",
  "org.slf4j" % "slf4j-api" % "2.0.7",
  "com.github.javafaker" % "javafaker" % "1.0.2",
  "org.apache.kafka" % "kafka-streams-test-utils" % "3.4.0" ,
  "org.hamcrest" % "hamcrest" % "2.2" % Test,
  "org.mockito" % "mockito-core" % "5.2.0" % Test,
  "org.mockito" % "mockito-junit-jupiter" % "5.2.0" % Test

)