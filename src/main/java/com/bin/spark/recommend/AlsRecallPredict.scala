package com.bin.spark.recommend

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import java.sql.{DriverManager, SQLException}
import scala.collection.JavaConverters._

object AlsRecallPredict {
  def main(args: Array[String]): Unit = {
    // 初始化Spark运行环境
    val spark = SparkSession.builder().master("local[*]").appName("SparkApp").getOrCreate()
    // 加载模型进内存
    val alsModel = ALSModel.load("file:///C:\\Users\\86178\\Desktop\\Search-and-Recommendation-main\\src\\main\\resources\\alsmodel")

    val csvFile = spark.read.textFile("file:///C:\\Users\\86178\\Desktop\\Search-and-Recommendation-main\\src\\main\\resources\\behavior.csv").rdd
    val ratingJavaRDD = csvFile.map(Rating.parseRating)
    val rating = spark.createDataFrame(ratingJavaRDD, classOf[Rating])

    // 给5个user做召回结果的预测
    val users = rating.select(alsModel.getItemCol).distinct().limit(500)
    val usersRecs = alsModel.recommendForItemSubset(users, 20)
    // 先分片
    usersRecs.foreachPartition { t =>
      // 建立数据库连接
      val username = "root"
      val password = "kytgm1314MH"
      val url = "jdbc:mysql://sh-cynosdbmysql-grp-5tpsbo9c.sql.tencentcdb.com:26211/spark_db?useUnicode=true&characterEncoding=utf-8&useSSL=false&connectTimeout=50000000"
      val connection = DriverManager.getConnection(url, username, password)

      val preparedStatement = connection.prepareStatement("insert into recommend(id,recommend) values (?,?)")
      var data = List[Map[String, Object]]()

      // 再遍历
      t.foreach { action =>
        // 这里才是Row值了
        val userId = action.getInt(0)
        val recommendationList = action.getList[GenericRowWithSchema](1)
        val shopIdList = recommendationList.asScala.map(_.getInt(0)).toList
        val recommend = shopIdList.mkString(",")

        val map = Map("userId" -> userId.asInstanceOf[Object], "recommend" -> recommend.asInstanceOf[Object])
        data = data :+ map
      }


      data.foreach { recommends =>
        try {
          preparedStatement.setInt(1, recommends("userId").toString.toInt)
          preparedStatement.setString(2, recommends("recommend").toString)
          preparedStatement.addBatch()
        } catch {
          case e: SQLException => e.printStackTrace()
        }
      }
      preparedStatement.executeBatch()
      connection.close()

    }
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
