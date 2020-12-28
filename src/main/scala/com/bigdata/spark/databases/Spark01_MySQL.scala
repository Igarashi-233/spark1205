package com.bigdata.spark.databases

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_MySQL {
  def main(args: Array[String]): Unit = {


    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataBase")

    val sc = new SparkContext(config)

    //定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "521314"

    //创建JdbcRDD 访问数据库

    /**
     * 查询数据
     *
     * val sql = "select name,age from user where id >= ? and id <= ?"
     * val jdbcRDD: JdbcRDD[Unit] = new JdbcRDD(
     * sc,
     * () => {
     * // 获取数据库连接对象
     * Class.forName(driver)
     * DriverManager.getConnection(url, userName, passWd)
     * },
     * sql,
     * 1,
     * 3,
     * 2,
     * rs => {
     * println(rs.getString(1) + "," + rs.getInt(2))
     * }
     * )
     *
     * jdbcRDD.collect()
     */

    /**
     * 保存数据
     */
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("D.va", 16), ("Mercy", 46), ("Ana", 53)), 2)

    // 效率低  无法序列化解决问题
    //    Class.forName(driver)
    //    val connection: Connection = DriverManager.getConnection(url, userName, passWd)
    //
    //    dataRDD.foreach {
    //      case (username, age) =>
    //
    //        val sql = "insert into user (name, age) values (?,?)"
    //        val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
    //
    //        preparedStatement.setString(1, username)
    //        preparedStatement.setInt(2, age)
    //
    //        preparedStatement.executeUpdate()
    //
    //        preparedStatement.close()
    //
    //    }
    //    connection.close()


    // 一个分区连接一次数据库
    dataRDD.foreachPartition(datas => {

      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)

      datas.foreach {
        case (username, age) =>

          val sql = "insert into user (name, age) values (?,?)"
          val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

          preparedStatement.setString(1, username)
          preparedStatement.setInt(2, age)

          preparedStatement.executeUpdate()

          preparedStatement.close()
      }
      connection.close()

    })

  }
}
