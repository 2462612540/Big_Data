package com.shanghaiuniversity.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // ����һ��spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    // ����spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // ��������
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(
        rating => Rating(rating.userId, rating.productId, rating.score)
      ).cache()

    // ���ݼ��зֳ�ѵ�����Ͳ��Լ�
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    // ����ʵ�֣�������Ų���
    adjustALSParams(trainingRDD, testingRDD)

    spark.stop()
  }

  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    // ���������ж���Ĳ���ȡֵ
    val result = for (rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01))
      yield {
        val model = ALS.train(trainData, rank, 10, lambda)
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    // ����rmse����������Ų���
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // ����userProducts���õ�Ԥ�����־���
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    // ���չ�ʽ����rmse�����Ȱ�Ԥ�����ֺ�ʵ�����ֱ���(userId, productId)��һ������
    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))

    sqrt(
      observed.join(predict).map {
        case ((userId, productId), (actual, pre)) =>
          val err = actual - pre
          err * err
      }.mean()
    )
  }
}
