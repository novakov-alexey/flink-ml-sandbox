package com.example

import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.conv.*

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.configuration.Configuration

import java.io.File
import scala.jdk.CollectionConverters.*

@main def readPaimonUtil(args: String*) =
  val cfg = Configuration.fromMap(Map("execution.runtime-mode" -> "batch").asJava)

  val env = StreamExecutionEnvironment.createLocalEnvironment(4, cfg)
  val tEnv = StreamTableEnvironment.create(env)

  lazy val currentDirectory = File(".").getCanonicalPath
  val catalogPath = if args.length > 2 then args(2) else s"file://$currentDirectory/target/catalog"
  val catalogName = "my_catalog"

  tEnv.executeSql(s"""
                     |CREATE CATALOG $catalogName WITH (
                     |    'type'='paimon',
                     |    'warehouse'='$catalogPath'
                     |);""".stripMargin)

  val tables = List("metricsSink", "evaluatorMetricsSink")
  tables.foreach { t =>
    println(s"$t content:")
    tEnv.executeSql(s"select * from $catalogName.`default`.$t").print()
  }
