package com.example

import org.apache.flinkx.api.StreamExecutionEnvironment

import org.apache.flink.ml.clustering.kmeans.{KMeans, KMeansModel}
import org.apache.flink.ml.regression.linearregression.LinearRegression
import org.apache.flink.ml.linalg.DenseVector
import org.apache.flink.ml.linalg.Vectors
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.TableDescriptor
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.types.Row
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.JobManagerOptions
import org.apache.flink.configuration.DeploymentOptions
import org.apache.flink.configuration.RestOptions
import org.apache.flink.configuration.StateBackendOptions
import org.apache.flink.client.deployment.executors.RemoteExecutor
import org.apache.flink.streaming.api.environment.{
  StreamExecutionEnvironment => JavaEnv
}

import scala.jdk.CollectionConverters.*
import java.lang.{Long as JLong}

import Common.*
import Common.given
import KmeansCommon.*

object Common:
  val labelCol = "label"
  val featuresCol = "features"
  val predictionCol = "prediction"
  val weightCol = "weight"

  given denseVectorTypeInfo: TypeInformation[DenseVector] =
    DenseVectorTypeInfo.INSTANCE

  given rowTypeInfo: TypeInformation[Row] = RowTypeInfo()

  val cfg = Configuration()
  cfg.setString("taskmanager.memory.network.max", "1g")

  val jmHost = "localhost"
  val restPort = 8081
  cfg.setString(JobManagerOptions.ADDRESS, jmHost)
  cfg.setInteger(JobManagerOptions.PORT, restPort)
  cfg.setString(DeploymentOptions.TARGET, RemoteExecutor.NAME)
  cfg.setBoolean(DeploymentOptions.ATTACHED, true)
  cfg.setString(RestOptions.ADDRESS, jmHost)
  cfg.setInteger(RestOptions.PORT, restPort)
  cfg.set(StateBackendOptions.STATE_BACKEND, "filesystem")

  val localMavenPath =
    s"${sys.props("user.home")}/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2"
  val jars = Array(
    s"$localMavenPath/org/apache/flink/flink-ml-uber-1.17/2.3.0/flink-ml-uber-1.17-2.3.0.jar",
    s"$localMavenPath/org/apache/flink/statefun-flink-core/3.2.0/statefun-flink-core-3.2.0.jar",
    "target/scala-3.3.4/my-flink-scala-proj_3-0.1.0-SNAPSHOT.jar"
  )

  val env = StreamExecutionEnvironment.createRemoteEnvironment(
    jmHost,
    restPort,
    cfg,
    jars: _*
  )
  
  val tEnv = StreamTableEnvironment.create(env.getJavaEnv)
  tEnv.createTemporarySystemFunction("doubleToVector", DoubleToVector())

object KmeansCommon:
  val modelPath = "target/trained-kmeans"
  val trainData = Seq(
    Vectors.dense(0.0, 0.0),
    Vectors.dense(0.0, 0.3),
    Vectors.dense(0.3, 0.0),
    Vectors.dense(9.0, 0.0),
    Vectors.dense(9.0, 0.6),
    Vectors.dense(9.6, 0.0)
  )

@main def KmeansTraining =
  val inputStream = tEnv
    .fromDataStream(
      env.fromCollection(trainData).javaStream
    )
    .as(featuresCol)

  val kmeans = KMeans()
    .setK(2)
    .setSeed(1L)
    .setFeaturesCol(featuresCol)
    .setPredictionCol(predictionCol)
  val model = kmeans.fit(inputStream)
  model.save(modelPath)
  env.execute("KMeans Training Job")

@main def KmeansInference =
  val model = KMeansModel.load(tEnv, modelPath)

  val inputStream = tEnv
    .fromDataStream(
      env
        .fromCollection(trainData.map(v => DenseVector(v.values.map(_ + 1d))))
        .javaStream
    )
    .as(featuresCol)
  val output = model.transform(inputStream)(0)

  for row <- output.execute().collect().asScala do
    val vector = row.getField(featuresCol).asInstanceOf[DenseVector]
    val clusterId = row.getField(predictionCol).asInstanceOf[Int]
    println(s"Vector: $vector \tCluster ID: $clusterId")

class DoubleToVector extends ScalarFunction:
  @DataTypeHint(value = "RAW", bridgedTo = classOf[DenseVector])
  def eval(d: java.lang.Double): DenseVector = Vectors.dense(d)

@main def LinearReg(sampleCount: Int) =
  val schema = Schema
    .newBuilder()
    .column("x", DataTypes.DOUBLE())
    .columnByExpression("y", "2 * x + 1")
    .build()

  val sample =
    tEnv
      .from(
        TableDescriptor
          .forConnector("datagen")
          .schema(schema)
          .option("fields.x.min", "1")
          .option("fields.x.max", "10")
          .option("number-of-rows", s"$sampleCount")
          .build()
      )

  val trainData = tEnv.sqlQuery(
    s"select doubleToVector(x) as $featuresCol, y as $labelCol from $sample"
  )

  val lr = LinearRegression().setLearningRate(0.01d)
  val model = lr.fit(trainData)
  println(s"Model has been trained")

  val testSample =
    tEnv
      .from(
        TableDescriptor
          .forConnector("datagen")
          .schema(schema)
          .option("fields.x.min", "1")
          .option("fields.x.max", "10")
          .option("number-of-rows", s"$sampleCount")
          .option(DataGenConnectorOptions.ROWS_PER_SECOND, new JLong(1))
          .build()
      )

  val testData = tEnv.sqlQuery(
    s"select doubleToVector(x) as $featuresCol, y as $labelCol from $testSample"
  )

  val output = model.transform(testData)(0)

  // streaming print
  println("features, label, prediction")
  output.execute().collect().forEachRemaining(println)

  // batch print
  // val result = output.execute().collect().asScala.toList
  // result.headOption.foreach(r =>
  //   println(r.getFieldNames(true).asScala.mkString(", "))
  // )
  // result.foreach(println)
