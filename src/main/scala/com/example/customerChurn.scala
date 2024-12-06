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
import org.apache.flink.ml.classification.linearsvc.LinearSVC
import org.apache.flink.ml.classification.logisticregression.LogisticRegression
import org.apache.flink.ml.evaluation.binaryclassification.BinaryClassificationEvaluator
import org.apache.flink.ml.evaluation.binaryclassification.{
  BinaryClassificationEvaluatorParams => ClassifierMetric
}
import org.apache.flink.table.api.*
import Common.*
import Common.given

import java.io.File
import scala.jdk.CollectionConverters.*

@main def customerChurn =
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
  val transformCols = List(
    "CreditScore",
    "Gender",
    "Age",
    "Tenure",
    "Balance",
    "NumOfProducts",
    "HasCrCard",
    "IsActiveMember",
    "EstimatedSalary"
  )

  val transformDoublesSql =
    transformCols.map(c => s"doubleToVector($c) as ${c}_v").mkString(",")

  val transformerStm =
    s"""SELECT 
    |Geography as Geography_v, 
    |$transformDoublesSql,    
    |$churnLabelCol FROM __THIS__""".stripMargin

  val sqlTransformer = SQLTransformer().setStatement(transformerStm)

  // 4 - Normalize numbers
  val continuesCols = List(
    "CreditScore",
    "Age",
    "Tenure",
    "Balance",
    "NumOfProducts",
    "EstimatedSalary"
  )

  val standardScalers = continuesCols
    .map(c =>
      StandardScaler()
        .setWithMean(false)
        .setInputCol(c + "_v")
        .setOutputCol(c + "_s")
    )

  // 5 - merge columns to features col
  val categoricalCols =
    List("Geography", "Gender", "HasCrCard", "IsActiveMember")
  val finalCols = categoricalCols.map(_ + "_v") ++ continuesCols.map(_ + "_s")
  // Geography is 3 countries + other 9 features
  val vectorSizes = 3 +: List.fill(finalCols.length - 1)(1)
  val vectorAssembler = VectorAssembler()
    .setInputCols(finalCols: _*)
    .setOutputCol(featuresCol)
    .setInputSizes(vectorSizes.map(Integer.valueOf): _*)

  // 6 - Train
  val lr = LogisticRegression()
    .setLearningRate(0.002d)
    .setLabelCol(churnLabelCol)
    .setReg(0.1)
    .setElasticNet(0.5)
    .setMaxIter(100)
    .setTol(0.01d)
    .setGlobalBatchSize(64)

  val stages = (List(
    indexer,
    geographyEncoder,
    sqlTransformer
  ) ++ standardScalers ++ List(vectorAssembler, lr))
    .map(_.asInstanceOf[Stage[?]])
    .asJava

  val pipeline = Pipeline(stages)

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
    "EstimatedSalary",
    churnLabelCol
  )
  val rawDataQuery =
    s"select ${rawFeatureCols.mkString(",")} from $trainData"
  val rawData = tEnv.sqlQuery(rawDataQuery)
  val testSetSize = 1000
  val trainSet = rawData.limit(9000)
  val testSet = rawData.limit(9000, testSetSize)

  val pipelineModel = pipeline.fit(trainSet)

  // Test
  val testResult = pipelineModel.transform(testSet)(0)

  val resQuery =
    s"""|select 
        |features, 
        |$churnLabelCol as label, 
        |prediction, 
        |rawPrediction        
        |from $testResult""".stripMargin
  val res = tEnv.sqlQuery(resQuery).execute
  val iter = res.collect

  val header = iter.next
  val colNames = header.getFieldNames(true).asScala.toList.mkString(", ")

  val correctCnt = iter.asScala.foldLeft(0) { (acc, row) =>
    println(row)
    val label = row.getFieldAs[Double]("label")
    val prediction = row.getFieldAs[Double]("prediction")
    if label == prediction then acc + 1 else acc
  }
  println(colNames)
  println(
    s"correct labels count: $correctCnt, accuracy: ${correctCnt / testSetSize.toDouble}"
  )

  val evaluator = BinaryClassificationEvaluator()
    .setLabelCol(churnLabelCol)
    .setMetricsNames(
      ClassifierMetric.AREA_UNDER_PR,
      ClassifierMetric.KS,
      ClassifierMetric.AREA_UNDER_ROC,
      ClassifierMetric.AREA_UNDER_LORENZ
    )

  // Uses the BinaryClassificationEvaluator object for evaluations.
  val outputTable = evaluator.transform(testResult)(0)
  val evaluationResult = outputTable.execute.collect.next
  println(
    s"Area under the precision-recall curve: ${evaluationResult.getField(ClassifierMetric.AREA_UNDER_PR)}"
  )
  println(
    s"Area under the receiver operating characteristic curve: ${evaluationResult
        .getField(ClassifierMetric.AREA_UNDER_ROC)}"
  )
  println(
    s"Kolmogorov-Smirnov value: ${evaluationResult.getField(ClassifierMetric.KS)}"
  )
  println(
    s"Area under Lorenz curve: ${evaluationResult.getField(ClassifierMetric.AREA_UNDER_LORENZ)}"
  )
