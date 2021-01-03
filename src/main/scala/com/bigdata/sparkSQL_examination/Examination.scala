package com.bigdata.sparkSQL_examination

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkConf

object Examination {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Examination")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // tbStock层数据加载
    println("=======================================tbStock=======================================")
    val tbStockRDD: RDD[String] = spark.sparkContext.textFile("tb/tbStock.txt")

    val tbStockDS: Dataset[tbStock] = tbStockRDD.map(_.split(",")).map(attr => {
      tbStock(attr(0), attr(1), attr(2))
    }).toDS()

    tbStockDS.show()
    tbStockDS.createTempView("tbStock")

    println("\n=======================================tbStockDetail=======================================")
    val tbStockDetailRDD: RDD[String] = spark.sparkContext.textFile("tb/tbStockDetail.txt")
    val tbStockDetailDS: Dataset[tbSrockDetail] = tbStockDetailRDD.map(_.split(",")).map(atter => {
      tbSrockDetail(atter(0), atter(1).trim.toInt, atter(2), atter(3).trim.toInt, atter(4).trim.toDouble, atter(5)
        .trim.toDouble)
    }).toDS()

    tbStockDetailDS.show()
    tbStockDetailDS.createTempView("tbStockDetail")

    println("\n=======================================tbDate=======================================")
    val tbDateRDD: RDD[String] = spark.sparkContext.textFile("tb/tbDate.txt")
    val tbDateDS: Dataset[tbDate] = tbDateRDD.map(_.split(",")).map(atter => {
      tbDate(atter(0), atter(1), atter(2).trim.toInt, atter(3).trim.toInt, atter(4).trim.toInt, atter(5).trim.toInt,
        atter(6).trim.toInt, atter(7).trim.toInt, atter(8).trim.toInt, atter(9).trim.toInt)
    }).toDS()

    tbDateDS.show()
    tbDateDS.createTempView("tbDate")


    /**
     * Target1
     * 计算所有订单中每年的销售单数、销售总额
     */
    spark.sql("select a.theyear,count(distinct b.ordernumber),sum(c.amount)\nfrom tbDate a\njoin tbStock b on a" +
      ".dateid = b.dateid\njoin tbStockDetail c on b.ordernumber = c.ordernumber\ngroup by a.theyear\norder by a.theyear").show()


    /**
     * Target2
     * 计算所有订单每年最大金额订单的销售额
     */
    spark.sql("select theyear,max(t1.SumOfAmount) SumOfAmount\nfrom(\nselect tbStock.dateid, tbStock.ordernumber, sum" +
      "(amount) SumOfAmount\nfrom tbStock\njoin tbStockDetail on tbStock.ordernumber=tbStockDetail.ordernumber\ngroup" +
      " by tbStock.dateid,tbStock.ordernumber\n) t1\njoin tbDate on tbDate.dateid=t1.dateid\ngroup by theyear\norder " +
      "by theyear").show()

    /**
     * Target3
     * 计算所有订单中每年最畅销货品
     */
    spark.sql("SELECT DISTINCT e.theyear, e.itemid, f.MaxOfAmount\nFROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS" +
      " SumOfAmount\n\tFROM tbStock a\n\t\tJOIN tbStockDetail b ON a.ordernumber = b.ordernumber\n\t\tJOIN tbDate c " +
      "ON a.dateid = c.dateid\n\tGROUP BY c.theyear, b.itemid\n\t) e\n\tJOIN (SELECT d.theyear, MAX(d.SumOfAmount) AS" +
      " MaxOfAmount\n\t\tFROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount\n\t\t\tFROM tbStock " +
      "a\n\t\t\t\tJOIN tbStockDetail b ON a.ordernumber = b.ordernumber\n\t\t\t\tJOIN tbDate c ON a.dateid = c" +
      ".dateid\n\t\t\tGROUP BY c.theyear, b.itemid\n\t\t\t) d\n\t\tGROUP BY d.theyear\n\t\t) f ON e.theyear = f" +
      ".theyear\n\t\tAND e.SumOfAmount = f.MaxOfAmount\nORDER BY e.theyear").show()

  }
}

case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable {

}

case class tbSrockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double)
  extends Serializable {

}

case class tbDate(dateid: String, years: String, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int,
                  period: Int, halfmonth: Int) extends Serializable {

}