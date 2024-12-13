package com.example

import org.apache.flink.ml.linalg.DenseVector
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo
import org.apache.flink.ml.linalg.Vectors
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.JobManagerOptions
import org.apache.flink.configuration.DeploymentOptions
import org.apache.flink.configuration.RestOptions
import org.apache.flink.configuration.StateBackendOptions
import org.apache.flink.client.deployment.executors.RemoteExecutor

import org.apache.flinkx.api.StreamExecutionEnvironment

object Common:
  val labelCol = "label"
  val featuresCol = "features"
  val predictionCol = "prediction"

  given denseVectorTypeInfo: TypeInformation[DenseVector] =
    DenseVectorTypeInfo.INSTANCE

  given rowTypeInfo: TypeInformation[Row] = RowTypeInfo()

  def getEnv(
      hostname: Option[String] = None
  ): (StreamExecutionEnvironment, StreamTableEnvironment) =
    val cfg = Configuration()

    val env = hostname match
      case Some(host) =>
        cfg.setString("taskmanager.memory.network.max", "1g")

        val restPort = 8081
        cfg.setString(JobManagerOptions.ADDRESS, host)
        cfg.setInteger(JobManagerOptions.PORT, restPort)
        cfg.setString(DeploymentOptions.TARGET, RemoteExecutor.NAME)
        cfg.setBoolean(DeploymentOptions.ATTACHED, true)
        cfg.setString(RestOptions.ADDRESS, host)
        cfg.setInteger(RestOptions.PORT, restPort)
        cfg.set(StateBackendOptions.STATE_BACKEND, "filesystem")

        val localMavenPath =
          s"${sys.props("user.home")}/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2"
        val jars = Array(
          s"$localMavenPath/org/apache/flink/flink-ml-uber-1.17/2.3.0/flink-ml-uber-1.17-2.3.0.jar",
          s"$localMavenPath/org/apache/flink/statefun-flink-core/3.2.0/statefun-flink-core-3.2.0.jar",
          "target/scala-3.3.4/my-flink-scala-proj_3-0.1.0-SNAPSHOT.jar"
        )
        val e = StreamExecutionEnvironment.createRemoteEnvironment(
          host,
          restPort,
          cfg,
          jars: _*
        )
        e.setParallelism(8)
        e

      case None =>
        StreamExecutionEnvironment.createLocalEnvironment(4, cfg)

    val tEnv = StreamTableEnvironment.create(env.getJavaEnv)
    tEnv.createTemporarySystemFunction("doubleToVector", DoubleToVector())
    (env, tEnv)

class DoubleToVector extends ScalarFunction:
  @DataTypeHint(value = "RAW", bridgedTo = classOf[DenseVector])
  def eval(d: java.lang.Double): DenseVector = Vectors.dense(d)
