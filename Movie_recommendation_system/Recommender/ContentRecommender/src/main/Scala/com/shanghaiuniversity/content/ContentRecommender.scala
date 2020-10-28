package com.shanghaiuniversity.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

// ��Ҫ������Դ�ǵ�Ӱ������Ϣ
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class MongoConfig(uri: String, db: String)

// ����һ����׼�Ƽ�����
case class Recommendation(mid: Int, score: Double)

// �����Ӱ������Ϣ��ȡ�������������ĵ�Ӱ���ƶ��б�
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object ContentRecommender {

  // ��������ͳ���
  val MONGODB_MOVIE_COLLECTION = "Movie"

  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

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

    // �������ݣ�����Ԥ����
    val movieTagsDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(
        // ��ȡmid��name��genres������Ϊԭʼ�����������ִ���Ĭ�ϰ��տո����ִ�
        x => (x.mid, x.name, x.genres.map(c => if (c == '|') ' ' else c))
      )
      .toDF("mid", "name", "genres")
      .cache()

    // ���Ĳ��֣� ��TF-IDF��������Ϣ����ȡ��Ӱ��������

    // ����һ���ִ�����Ĭ�ϰ��ո�ִ�
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")

    // �÷ִ�����ԭʼ������ת���������µ�һ��words
    val wordsData = tokenizer.transform(movieTagsDF)

    // ����HashingTF���ߣ����԰�һ����������ת���ɶ�Ӧ�Ĵ�Ƶ
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(wordsData)

    // ����IDF���ߣ����Եõ�idfģ��
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // ѵ��idfģ�ͣ��õ�ÿ���ʵ����ĵ�Ƶ��
    val idfModel = idf.fit(featurizedData)
    // ��ģ�Ͷ�ԭ���ݽ��д����õ��ĵ���ÿ���ʵ�tf-idf����Ϊ�µ���������
    val rescaledData = idfModel.transform(featurizedData)

    //    rescaledData.show(truncate = false)

    val movieFeatures = rescaledData.map(
      row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
    )
      .rdd
      .map(
        x => (x._1, new DoubleMatrix(x._2))
      )
    movieFeatures.collect().foreach(println)

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
      .option("collection", CONTENT_MOVIE_RECS)
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
