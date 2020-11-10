package mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将相关的数据的保存早的mysql中
 */
object MysqlWriteTest {
  def main(args: Array[String]): Unit = {
    //TODO 构建一个spark的对象
    //构建Spark Application 应用的入口实例
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    //TODO spark 连接数据库
    val inputpath = ""
    //1 读取数据
    val inputRDD: RDD[String] = sc.textFile(inputpath)
    //2处理分析数据的 调用的RDD中的transformation函数
    val resultRDD: RDD[(String, Int)] = inputRDD
      //过滤空数据
      .filter(line => null != line && line.trim.length != 0)
      //分割单词
      .flatMap(line => line.trim.split("\\s+"))
      //转为二元组 表示的是每一个单词的出现的次数
      .map(word => word -> 1)
      //分组聚合
      .reduceByKey((tmp, item) => tmp + item)

    resultRDD
      //降低分区数
      .coalesce(numPartitions = 1)
      .foreachPartition({ iter => saveToMYSQL(iter)
      })

    //TODO 关闭的spark资源
    sc.stop()
  }

  /**
   * 将RDD每一个分区数据的保存到MYSQL表汇总
   *
   * @param datas
   */
  def saveToMYSQL(datas: Iterator[(String, Int)]) = {
    //加载驱动
    Class.forName("")
    var conn: Connection = null
    var pstmt: PreparedStatement = null;
    try {
      //获取连接
      conn = DriverManager.getConnection(
        "",
        "",
        ""
      )

      //获取事务级别
      val autoConmmit = conn.getAutoCommit
      conn.setAutoCommit(false)
      //插入数据
      val insertsql = ""
      pstmt = conn.prepareStatement(insertsql)
      datas.foreach({ case (word, count) =>
        pstmt.setString(1, word)
        pstmt.setString(2, count.toString)
        //加入批量操作
        pstmt.addBatch()
      })
      //批次插入
      pstmt.executeBatch()
      //手动提交
      conn.commit()
      //还原数据库的原来的事务级别状态
      conn.setAutoCommit(autoConmmit)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      //关闭连接
      if (null != pstmt) pstmt.close()
      if (null != conn) conn.close()
    }
  }

}
