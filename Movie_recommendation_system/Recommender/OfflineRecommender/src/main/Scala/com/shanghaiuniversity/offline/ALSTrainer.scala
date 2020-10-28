package com.shanghaiuniversity.offline

import breeze.numerics.sqrt

import com.shanghaiuniversity.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * ���ڵ�ALSģ�͵������Ͳ�����ѡ��
 */
object ALSTrainer {

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.25.131:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    // ����һ��SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // ������������
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)) // ת����rdd������ȥ��ʱ���
      .cache()

    // ����з����ݼ�������ѵ�����Ͳ��Լ�
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    // ģ�Ͳ���ѡ��������Ų���
    adjustALSParam(trainingRDD, testRDD)

    spark.close()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
    val result = for (rank <- Array(50, 100, 200, 300); lambda <- Array(0.01, 0.1, 1))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)
        // ���㵱ǰ������Ӧģ�͵�rmse������Double
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    // ����̨��ӡ������Ų���
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // ����Ԥ������
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    // ��uid��mid��Ϊ�����inner joinʵ�ʹ۲�ֵ��Ԥ��ֵ
    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))
    // �����ӵõ�(uid, mid),(actual, predict)
    sqrt(
      observed.join(predict).map {
        case ((uid, mid), (actual, pre)) =>
          val err = actual - pre
          err * err
      }.mean()
    )
  }
}
