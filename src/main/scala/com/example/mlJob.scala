package com.example

import com.example.Common.{*, given}
import com.example.KmeansCommon.*
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions
import org.apache.flink.ml.clustering.kmeans.{KMeans, KMeansModel}
import org.apache.flink.ml.linalg.{DenseVector, Vectors}
import org.apache.flink.ml.regression.linearregression.LinearRegression
import org.apache.flink.table.api.{DataTypes, Schema, TableDescriptor}
import org.apache.flinkx.api.StreamExecutionEnvironment

import java.lang.Long as JLong
import scala.jdk.CollectionConverters.*

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
  val (env, tEnv) = getEnv()
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
  val (env, tEnv) = getEnv()
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

@main def LinearReg(sampleCount: Int) =
  val schema = Schema
    .newBuilder()
    .column("x", DataTypes.DOUBLE())
    .columnByExpression("y", "2 * x + 1")
    .build()

  val (_, tEnv) = getEnv()
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
          .option(DataGenConnectorOptions.ROWS_PER_SECOND, JLong.valueOf(1))
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
