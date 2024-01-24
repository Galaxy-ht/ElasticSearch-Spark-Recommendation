package com.bin.spark.recommend

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object LrTrain {
  def main(args: Array[String]): Unit = {
    // 初始化Spark运行环境
    val spark = SparkSession.builder().master("local").appName("SparkApp").getOrCreate()

    val csvFile = spark.read.textFile("file:///Users/mac/Desktop/data/feature.csv").rdd

    val ratingJavaRDD = csvFile.map { v1 =>
      val strArr = v1.replace("\"", "").split(",")
      Row(
        new java.lang.Double(strArr(11)),
        Vectors.dense(strArr.slice(0, 11).map(_.toDouble))
      )
    }

    val schema = new StructType(
      Array(
        new StructField("label", DataTypes.DoubleType, false, Metadata.empty),
        new StructField("features", new VectorUDT, false, Metadata.empty)
      )
    )

    val data = spark.createDataFrame(ratingJavaRDD, schema)

    val dataArr = data.randomSplit(Array(0.8, 0.2))
    val trainData = dataArr(0)
    val testData = dataArr(1)

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setFamily("multinomial")
    val lrModel: LogisticRegressionModel = lr.fit(trainData)

    // 保存lrModel
    lrModel.save("file:///Users/mac/Desktop/data/lrModel")

    // 测试评估
    val predictions = lrModel.transform(testData)

    // 评价指标
    val evaluator = new MulticlassClassificationEvaluator()
    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)

    println("accuracy=" + accuracy)
  }
}

