lazy val root = (project in file("."))
  .settings(
    name := "EchatKafkaMonitor",

    version := "0.0.1",

    scalaVersion := "2.12.6",

    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17",
      "org.mockito" % "mockito-all" % "1.10.19" % "test",
      "org.apache.kafka" % "kafka_2.12" % "1.0.0",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" % "scala-logging_2.12" % "3.9.0"),
    resolvers ++= Seq(
      "Nexus aliyun" at "http://maven.aliyun.com/nexus/content/groups/public")
  )