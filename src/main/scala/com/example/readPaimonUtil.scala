package com.example

import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.conv.*

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.configuration.Configuration
import java.io.File

@main def readPaimonUtil(args: String*) =
  val cfg = Configuration()
  cfg.setString("execution.runtime-mode", "batch")

  val env = StreamExecutionEnvironment.createLocalEnvironment(4, cfg)
  val tEnv = StreamTableEnvironment.create(env)

  lazy val currentDirectory = File(".").getCanonicalPath
  val catalogPath = if args.length > 2 then args(2) else s"file://$currentDirectory/target/catalog"
  tEnv.executeSql(s"""
                     |CREATE CATALOG my_catalog WITH (
                     |    'type'='paimon',
                     |    'warehouse'='$catalogPath'
                     |);""".stripMargin)  

  println(s"metricsSink content:")
  tEnv.executeSql("select * from my_catalog.`default`.metricsSink").print()

  println(s"evaluatorMetricsSink content:")
  tEnv.executeSql("select * from my_catalog.`default`.evaluatorMetricsSink").print()
