package com.shanghaiuniversity.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

// �����������ݵ�LFM��ֻ��Ҫrating����
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

// ����һ����׼�Ƽ�����
case class Recommendation(mid: Int, score: Double)

// �������Ԥ�����ֵ��û��Ƽ��б�
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// �������LFM��Ӱ���������ĵ�Ӱ���ƶ��б�
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

//MongoDB�����ӵ�����
case class MongoConfig(uri: String, db: String)

object OfflineRecommender {

  // ��������ͳ���  ��ȡ��
  val MONGODB_RATING_COLLECTION = "Rating"
  //���ɵı�����
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

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

    // ��������
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)) // ת����rdd������ȥ��ʱ���
      .cache()

    // ��rating��������ȡ���е�uid��mid����ȥ��
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    // ѵ��������ģ��
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    val (rank, iterations, lambda) = (200, 5, 0.1)
    val model = ALS.train(trainData, rank, iterations, lambda)

    // �����û��͵�Ӱ��������������Ԥ�����֣��õ��û����Ƽ��б�
    // ����user��movie�ĵѿ��������õ�һ�������־���
    val userMovies = userRDD.cartesian(movieRDD)

    // ����model��predict����Ԥ������
    val preRatings = model.predict(userMovies)

    val userRecs = preRatings
      .filter(_.rating > 0) // ���˳����ִ���0����
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // ���ڵ�Ӱ���������������ƶȾ��󣬵õ���Ӱ�����ƶ��б�
    val movieFeatures = model.productFeatures.map {
      case (mid, features) => (mid, new DoubleMatrix(features))
    }

    // �����е�Ӱ�����������ǵ����ƶȣ������ѿ�����
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter {
        // ���Լ����Լ�����Թ��˵�
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) => {
          val simScore = this.consinSim(a._2, b._2)
          (a._1, (b._1, simScore))
        }
      }
      .filter(_._2._2 > 0.6) // ���˳����ƶȴ���0.6��
      .groupByKey()
      .map {
        case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()
    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    spark.stop()
  }

  // �������������ƶ�
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }
}
