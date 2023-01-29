package de.ddm

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions

    val dataset = inputs.map(input => readData(input, spark).withColumn("filename",functions.lit(input)))
      .map(ds => {
      val columns = ds.columns.filter(x => !x.equals("filename"))
      ds.flatMap(row => {
        for (i <- columns.indices) yield {
          (columns(i), row.getString(i), row.getString(columns.length))
        }
      })
        .groupByKey(x => x._1)
        .mapGroups { case (k, iter) =>
          val listBuffer1 = new ListBuffer[String]
          val listBuffer2 = new ListBuffer[String]
          for (a <- iter) {
            listBuffer1 += a._2
            listBuffer2 += a._3
          }
          (k, listBuffer1.toSet, listBuffer2.head)
        }})

    println("#################################")
    println("Data saved as datasets. Start running INDS")
    println("#################################")

      dataset.flatMap(ds => dataset.map(x => x.crossJoin(ds).filter(x => !x.getString(0).equals(x.getString(3)))))
        .map(dataset => dataset
         .map(row => (row.getString(2),row.getString(5), row.getString(0),row.getString(3), row.getList(4).containsAll(row.getList(1)))))
         .foreach(x => x.foreach(row => if(row._5) {
            println(row._1 + " -> " + row._2 + "[" + row._3 + "]" + " c " + "[" + row._4 + "]")
          }
         ))
  }
}
