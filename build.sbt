Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / organization := "com.example"
ThisBuild / scalaVersion := "3.3.4"

val flinkVersion = "1.18.1"
val flinkMlVersion = "2.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "flink-ml-sandbox",
    run / javaOptions += "-Xmx4G",
    fork := true,
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-ml-uber-1.17" % flinkMlVersion % Provided,
      ("org.apache.flink" % "statefun-flink-core" % "3.2.0")
        .exclude("org.apache.flink", "flink-streaming-java_2.12")
        .exclude("org.apache.flink", "flink-metrics-dropwizard") % Provided,
      "org.flinkextended" %% "flink-scala-api" % s"1.2.3-SNAPSHOT",
      "org.apache.flink" % "flink-runtime-web" % flinkVersion % Provided,
      "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion % Provided,
      "org.apache.flink" % "flink-connector-files" % flinkVersion % Provided,
      "org.apache.flink" % "flink-csv" % flinkVersion % Provided,
      "org.apache.flink" % "flink-clients" % flinkVersion % Provided,
      "org.apache.flink" % "flink-table-runtime" % flinkVersion % Provided,
      "org.apache.flink" % "flink-table-planner-loader" % flinkVersion % Provided,
      "ch.qos.logback" % "logback-classic" % "1.4.14" % Provided
    ),
    buildInfoPackage := "com.example",
    buildInfoKeys := Seq[BuildInfoKey](
      "jarPath" -> (Compile / packageBin / artifactPath).value
    )
  )
  .enablePlugins(BuildInfoPlugin)

// make run command include the provided dependencies
Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true
