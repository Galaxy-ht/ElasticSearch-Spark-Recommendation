package com.bin.spark.recommend

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object AlsRecallTrain extends Serializable {

  def main(args: Array[String]): Unit = {
    // 初始化Spark运行环境
    val spark = SparkSession.builder().master("local").appName("SparkApp").getOrCreate()

    val csvFile = spark.read.textFile("file:///Users/mac/Desktop/data/behavior.csv").rdd
    val ratingJavaRDD = csvFile.map(Rating.parseRating)
    // 变为DataSet<Row> 数据就写入spark计算集合系统中了
    val rating: Dataset[Row] = spark.createDataFrame(ratingJavaRDD, classOf[Rating])

    // 将所有的rating数据分成82份
    val ratings: Array[Dataset[Row]] = rating.randomSplit(Array(0.8, 0.2))
    val trainingData: Dataset[Row] = ratings(0)
    val testingData: Dataset[Row] = ratings(1)

    // 过拟合：增大数据规模、减少rank，增大正则化的系数
    // 欠拟合：增加rank，减少正则化系数
    val als: ALS = new ALS().setMaxIter(10).setRank(5).setRegParam(0.01).
      setUserCol("userId").setItemCol("shopId").setRatingCol("rating")

    // 模型训练
    val alsModel: ALSModel = als.fit(trainingData)

    // 模型评测
    val predictions: Dataset[Row] = alsModel.transform(testingData)

    // rmse 均方差根误差，预测值与真实值之间误差的平方和除以观测次数，开个根号
    val evaluator: RegressionEvaluator = new RegressionEvaluator().setMetricName("rmse")
      .setLabelCol("rating").setPredictionCol("prediction")
    val rmse: Double = evaluator.evaluate(predictions)
    println(s"rmse = $rmse")

    // 保存数据模型
    alsModel.save("file:///Users/mac/Desktop/data/alsModel")
  }

  case class Rating(userId: Int, shopId: Int, rating: Int)

  object Rating {
    def parseRating(str: String): Rating = {
      val cleanedStr = str.replaceAll("\"", "")
      val strArr = cleanedStr.split(",")
      Rating(
        userId = strArr(0).toInt,
        shopId = strArr(1).toInt,
        rating = strArr(2).toInt
      )
    }
  }
}

