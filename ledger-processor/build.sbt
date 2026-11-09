ThisBuild / scalaVersion := "2.13.15"
ThisBuild / organization := "com.ledger"
ThisBuild / version      := "1.0.0"

val AkkaVersion      = "2.6.21"  // last Apache-2.0 licensed Akka line
val AkkaHttpVersion  = "10.2.10"
val AlpakkaKafka     = "2.1.1"
val CirceVersion     = "0.14.10"

lazy val root = (project in file("."))
  .settings(
    name := "ledger-processor",
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint:-byname-implicit,_", "-Wunused:imports"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka"   %% "akka-stream"        % AkkaVersion,
      "com.typesafe.akka"   %% "akka-slf4j"         % AkkaVersion,
      "com.typesafe.akka"   %% "akka-stream-kafka"  % AlpakkaKafka,
      "com.typesafe.akka"   %% "akka-http"          % AkkaHttpVersion,
      "io.circe"            %% "circe-core"         % CirceVersion,
      "io.circe"            %% "circe-parser"       % CirceVersion,
      "org.typelevel"       %% "cats-core"          % "2.12.0",
      "org.postgresql"       % "postgresql"         % "42.7.4",
      "com.zaxxer"           % "HikariCP"           % "5.1.0",
      "ch.qos.logback"       % "logback-classic"    % "1.2.13",
      "net.logstash.logback" % "logstash-logback-encoder" % "6.6",
      "org.scalatest"       %% "scalatest"          % "3.2.19"    % Test,
      "org.typelevel"       %% "cats-kernel-laws"   % "2.12.0"    % Test,
      "org.typelevel"       %% "discipline-scalatest" % "2.3.0"   % Test,
      "com.typesafe.akka"   %% "akka-stream-testkit" % AkkaVersion % Test
    ),
    Test / fork := true,
    run / fork  := true,
    assembly / mainClass := Some("ledger.processor.Main"),
    assembly / assemblyJarName := "ledger-processor.jar",
    assembly / assemblyMergeStrategy := {
      case "reference.conf"                         => MergeStrategy.concat
      case "application.conf"                       => MergeStrategy.concat
      case PathList("META-INF", "versions", _ @ _*) => MergeStrategy.first
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
      case "module-info.class"                      => MergeStrategy.discard
      case x                                        => MergeStrategy.first
    }
  )
