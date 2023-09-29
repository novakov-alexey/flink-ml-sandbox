Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / organization := "com.example"
ThisBuild / scalaVersion := "3.3.0"

val flinkVersion = "1.17.1"
val flinkMlVersion = "2.3.0"

lazy val root = (project in file(".")).settings(
  name := "my-flink-scala-proj",
  libraryDependencies ++= Seq(
    "org.apache.flink" % "flink-ml-uber-1.17" % flinkMlVersion % Provided,
    ("org.apache.flink" % "statefun-flink-core" % "3.2.0")
      .exclude("org.apache.flink", "flink-streaming-java_2.12")
      .exclude("org.apache.flink", "flink-metrics-dropwizard") % Provided,
    "org.flinkextended" %% "flink-scala-api" % s"${flinkVersion}_1.1.0",
    "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion % Provided,
    "org.apache.flink" % "flink-connector-files" % flinkVersion % Provided,
    "org.apache.flink" % "flink-csv" % flinkVersion % Provided,
    "org.apache.flink" % "flink-clients" % flinkVersion % Provided,
    "org.apache.flink" % "flink-table-runtime" % flinkVersion % Provided,
    "org.apache.flink" % "flink-table-planner-loader" % flinkVersion % Provided
  ),
  assemblyMergeStrategy := {
    case PathList(ps @ _*) if ps.last endsWith "module-info.class" =>
      MergeStrategy.first
    case x =>
      val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  assembly / assemblyExcludedJars := {
    val cp = (assembly / fullClasspath).value
    cp filter { f =>
      Set(
        "scala-asm-9.3.0-scala-1.jar",
        "interface-1.0.12.jar",
        "jline-terminal-3.19.0.jar",
        "jline-reader-3.19.0.jar",
        "jline-3.21.0.jar",
        "scala-compiler-2.13.10.jar",
        "flink-shaded-zookeeper-3-3.7.1-16.1.jar"
      ).contains(
        f.data.getName
      )
    }
  }
)

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
