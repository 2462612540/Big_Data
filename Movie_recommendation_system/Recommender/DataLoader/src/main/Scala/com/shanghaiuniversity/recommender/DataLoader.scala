package com.shanghaiuniversity.recommender

import java.net.InetAddress
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

//----------------------------------------------------------------------------------------------------------------------
//������Ķ���
/**
  * Movie ���ݼ�
  *
  * 260                                         ��ӰID��mid
  * Star Wars: Episode IV - A New Hope (1977)   ��Ӱ���ƣ�name
  * Princess Leia is captured and held hostage  ����������descri
  * 121 minutes                                 ʱ����timelong
  * September 21, 2004                          ����ʱ�䣬issue
  * 1977                                        ����ʱ�䣬shoot
  * English                                     ���ԣ�language
  * Action|Adventure|Sci-Fi                     ���ͣ�genres
  * Mark Hamill|Harrison Ford|Carrie Fisher     ��Ա��actors
  * George Lucas                                ���ݣ�directors
  *
  */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

/**
  * Rating���ݼ�
  *
  * 1,31,2.5,1260759144  �û�ID  ��ӰID  ����   ʱ���
  */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
  * Tag���ݼ�
  *
  * 15,1955,dentist,1193435061
  */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

//----------------------------------------------------------------------------------------------------------------------
// ��mongo��es�����÷�װ��������

/**
  *
  * @param uri MongoDB����
  * @param db  MongoDB���ݿ�
  */
case class MongoConfig(uri: String, db: String)

/**
  *
  * @param httpHosts      http�����б����ŷָ�
  * @param transportHosts transport�����б�
  * @param index          ��Ҫ����������
  * @param clustername    ��Ⱥ���ƣ�Ĭ��elasticsearch
  */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clustername: String)

object DataLoader {

  // ���峣��
  val MOVIE_DATA_PATH = "E:\\GItHub_project\\Big_Data\\Movie_recommendation_system\\Recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "E:\\GItHub_project\\Big_Data\\Movie_recommendation_system\\Recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "E:\\GItHub_project\\Big_Data\\Movie_recommendation_system\\Recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"


  //�����������
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.25.131:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "192.168.25.131:9200",
      "es.transportHosts" -> "192.168.25.131:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )

    // ����һ��sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    // ����һ��SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // ��������
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(
      item => {
        val attr = item.split("\\^")
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
      }
    ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // �����ݱ��浽MongoDB
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    // ����Ԥ������movie��Ӧ��tag��Ϣ��ӽ�ȥ����һ�� tag1|tag2|tag3...
    import org.apache.spark.sql.functions._

    /**
      * mid, tags
      *
      * tags: tag1|tag2|tag3...
      */
    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    // newTag��movie��join�����ݺϲ���һ����������
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))

    // �������ݵ�ES
    storeDataInES(movieWithTagsDF)

    spark.stop()
  }

  //�洢���ݵ�MOngoDB��
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // �½�һ��mongodb������
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // ���mongodb���Ѿ�����Ӧ�����ݿ⣬��ɾ��
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    // ��DF����д���Ӧ��mongodb����
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //�����ݱ�����
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    //�ر�����
    mongoClient.close()
  }

  //�洢���ݵ�ElasticSerach��
  def storeDataInES(movieDF: DataFrame)(implicit eSConfig: ESConfig): Unit = {
    // �½�es����
    val settings: Settings = Settings.builder().put("cluster.name", eSConfig.clustername).build()

    // �½�һ��es�ͻ���
    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    // ����������������
    if (esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index))
      .actionGet()
      .isExists
    ) {
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }

    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    movieDF.write
      .option("es.nodes", eSConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index + "/" + ES_MOVIE_INDEX)
  }
}

