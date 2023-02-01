package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

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

    inputs.map(input => readData(input, spark))
      .map(inputTable => {
        val columns = inputTable.columns
        inputTable.flatMap(row => {
          for (i <- columns.indices) yield {
            (columns(i), row.getString(i))
          }
        })
      })
      .reduce((firstDataset, secondDataSet) => firstDataset union secondDataSet)
      .groupByKey(tuple => tuple._2)
      .mapGroups { case (key, iter) =>
        val listBuffer1 = new ListBuffer[String]
        for (i <- iter) {
          listBuffer1 += i._1
        }
        (listBuffer1.toSet)
      }
      .flatMap(attributeSet => attributeSet
        .map(currentAttribute =>
          (currentAttribute, attributeSet.filter(attribute => attribute != currentAttribute))))
      .groupByKey(row => row._1)
      .mapGroups((key, iter) =>
        (key, iter.map(row => row._2).reduce((firstSet, secondSet) => firstSet.intersect(secondSet))))
      .filter(row => row._2.nonEmpty)
      .collect()
      .sortBy(ind => ind._1)
      .foreach(ind => println(ind._1 + " < " + ind._2.mkString(", ")))
  }
}
