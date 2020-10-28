package com.shanghaiuniversity.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int )

case class MongoConfig(uri:String, db:String)

// ����һ����׼�Ƽ�����
case class Recommendation( mid: Int, score: Double )

// �����Ӱ���top10�Ƽ�����
case class GenresRecommendation( genres: String, recs: Seq[Recommendation] )


object StatisticsRecommender {

  // �������   ��ȡ�ı�
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //ͳ�Ƶı������  ���������ݵı�
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.25.131:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // ����һ��sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommeder")

    // ����һ��SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // ��mongodb��������
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // ������Ϊratings����ʱ��
    ratingDF.createOrReplaceTempView("ratings")

    // ��ͬ��ͳ���Ƽ����
    // 1. ��ʷ����ͳ�ƣ���ʷ����������࣬mid��count
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")
    // �ѽ��д���Ӧ��mongodb����
    storeDFInMongoDB( rateMoreMoviesDF, RATE_MORE_MOVIES )

    // 2. ��������ͳ�ƣ����ա�yyyyMM����ʽѡȡ������������ݣ�ͳ�����ָ���
    // ����һ�����ڸ�ʽ������
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // ע��udf����ʱ���ת�������¸�ʽ
    spark.udf.register("changeDate", (x: Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt )

    // ��ԭʼ������Ԥ����ȥ��uid
    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    // ��ratingOfMonth�в��ҵ�Ӱ�ڸ����·ݵ����֣�mid��count��yearmonth
    val rateMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")

    // ����mongodb
    storeDFInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // 3. ���ʵ�Ӱͳ�ƣ�ͳ�Ƶ�Ӱ��ƽ�����֣�mid��avg
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    // 4. ������ӰTopͳ��
    // �����������
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    // ��ƽ�����ּ���movie�����һ�У�inner join
    val movieWithScore = movieDF.join(averageMoviesDF, "mid")

    // Ϊ���ѿ���������genresת��rdd
    val genresRDD = spark.sparkContext.makeRDD(genres)

    // �������top10�����ȶ����͵�Ӱ���ѿ�����
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        // �������ˣ��ҳ�movie���ֶ�genresֵ(Action|Adventure|Sci-Fi)������ǰ���genre(Action)����Щ
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains( genre.toLowerCase )
      }
      .map{
        case (genre, movieRow) => ( genre, ( movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg") ) )
      }
      .groupByKey()
      .map{
        case (genre, items) => GenresRecommendation( genre, items.toList.sortWith(_._2>_._2).take(10).map( item=> Recommendation(item._1, item._2)) )
      }
      .toDF()

    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
