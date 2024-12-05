package com.example

import org.apache.flink.ml.regression.linearregression.LinearRegression
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoder
import org.apache.flink.ml.feature.stringindexer.{
  StringIndexer,
  StringIndexerParams
}
import org.apache.flink.ml.feature.standardscaler.StandardScaler
import org.apache.flink.ml.feature.sqltransformer.SQLTransformer
import org.apache.flink.ml.feature.vectorassembler.VectorAssembler
import org.apache.flink.ml.builder.Pipeline
import org.apache.flink.ml.api.{Estimator, Stage}
import org.apache.flink.table.api.*
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.annotation.DataTypeHint
import Common.*
import Common.given

import java.io.File
import scala.jdk.CollectionConverters.*

def predictedToBinary[T](v: Double): Short =
  if v > 0.5 then 1 else 0

class PredictedToBinary extends ScalarFunction:
  @DataTypeHint(value = "RAW", bridgedTo = classOf[Short])
  def eval(d: java.lang.Double): Short = if d > 0.5 then 1 else 0

@main def customerChurn =
  tEnv.createTemporarySystemFunction("predictedToBinary", PredictedToBinary())

  val churnLabelCol = "Exited"
  val schema = Schema
    .newBuilder()
    .column("RowNumber", DataTypes.INT())
    .column("CustomerId", DataTypes.INT())
    .column("Surname", DataTypes.STRING())
    .column("CreditScore", DataTypes.DOUBLE())
    .column("GeographyStr", DataTypes.STRING())
    .column("GenderStr", DataTypes.STRING())
    .column("Age", DataTypes.DOUBLE())
    .column("Tenure", DataTypes.DOUBLE())
    .column("Balance", DataTypes.DOUBLE())
    .column("NumOfProducts", DataTypes.DOUBLE())
    .column("HasCrCard", DataTypes.DOUBLE())
    .column("IsActiveMember", DataTypes.DOUBLE())
    .column("EstimatedSalary", DataTypes.DOUBLE())
    .column(churnLabelCol, DataTypes.DOUBLE())
    .build()

  val currentDirectory = File(".").getCanonicalPath
  val trainData = tEnv.from(
    TableDescriptor
      .forConnector("filesystem")
      .schema(schema)
      .option("path", s"file://${currentDirectory}/data/Churn_Modelling.csv")
      //   .option("path", s"s3://vvp/artifacts/namespaces/default/Churn_Modelling.csv")
      .option("format", "csv")
      .option("csv.allow-comments", "true")
      .build()
  )

  val rawFeatureCols = List(
    "CreditScore",
    "GeographyStr",
    "GenderStr",
    "Age",
    "Tenure",
    "Balance",
    "NumOfProducts",
    "HasCrCard",
    "IsActiveMember",
    "EstimatedSalary"
  )
  val rawData = tEnv
    .sqlQuery(s"""select ${(rawFeatureCols :+ churnLabelCol).mkString(",")} 
      |from $trainData""".stripMargin)

  // 1 - index Geography and Gender
  val indexer = StringIndexer()
    .setStringOrderType(StringIndexerParams.ALPHABET_ASC_ORDER)
    .setInputCols("GeographyStr", "GenderStr")
    .setOutputCols("GeographyInd", "Gender")

  // 2 - OneHot Encode Geography
  val geographyEncoder =
    OneHotEncoder()
      .setInputCols("GeographyInd")
      .setOutputCols("Geography")
      .setDropLast(false)

  // 3 - Transform Double to Vector
  val transformCols = rawFeatureCols.filterNot(_.endsWith("Str"))
  val transformDoublesSql =
    transformCols.map(c => s"doubleToVector($c) as ${c}_v").mkString(",")

  val sqlTransformer = SQLTransformer().setStatement(
    s"SELECT Geography, Gender, $transformDoublesSql, $churnLabelCol FROM __THIS__"
  )

  // 4 - Normalize numbers
  val standardScalers =
    transformCols.map(c =>
      StandardScaler()
        .setWithMean(true)
        .setInputCol(c + "_v")
        .setOutputCol(c + "_s")
    )

  // 5 - merge columns to features col
  val finalCols = List("Geography", "Gender") ++ transformCols.map(_ + "_s")
  val vectorSizes = 3 +: List.fill(finalCols.length - 1)(1)
  val vectorAssembler = VectorAssembler()
    .setInputCols(finalCols: _*)
    .setOutputCol(featuresCol)
    .setInputSizes(vectorSizes.map(Integer.valueOf): _*)

  val stages = (List(
    indexer,
    geographyEncoder,
    sqlTransformer
  ) ++ standardScalers :+ vectorAssembler)
    .map(_.asInstanceOf[Stage[?]])
    .asJava

  val pipeline = Pipeline(stages)
  val pipelineModel = pipeline.fit(rawData)
  val transformedData = pipelineModel.transform(rawData)(0)

  val finalQuery =
    s"select $featuresCol, $churnLabelCol from $transformedData"
  val finalTrainData = tEnv.sqlQuery(finalQuery)

  // Train
  val lr = LinearRegression()
    .setLearningRate(0.002d)
    .setLabelCol(churnLabelCol)
    .setMaxIter(100)
    .setGlobalBatchSize(64)

  val trainSet = finalTrainData.limit(8000)
  val testSet = finalTrainData.limit(8000, 2000)
  val lrModel = lr.fit(trainSet)

  // Test
  val testResult = lrModel.transform(testSet)(0)

  val resQuery =
    s"""|select 
        |features, $churnLabelCol as label, prediction as rawPredicted, 
        |predictedToBinary(prediction) as predicted 
        |from $testResult""".stripMargin
  val res = tEnv.sqlQuery(resQuery).execute
  val iter = res.collect

  val header = iter.next
  val colNames = header.getFieldNames(true).asScala.toList.mkString(", ")

  iter.forEachRemaining(println)
  println(colNames)
