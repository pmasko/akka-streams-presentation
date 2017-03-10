name := "akka-streams-at-so1"

version := "1.0"

scalaVersion := "2.11.8"


val akkaStreamsVersion = "2.4.17"

val akkaVersion = "2.4.17"
val alpakkaVersion = "0.6"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-agent" % akkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % alpakkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream" % akkaStreamsVersion,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaStreamsVersion % "test",
  "org.scalatest"     %% "scalatest"           % "3.0.1"       % Test,
  "com.novocode"       % "junit-interface"     % "0.11"        % Test,
  "junit"              % "junit"               % "4.12"        % Test
)
