package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    val region = spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path = inputs(0))
      .toDF(colNames = "R_Region_Key", "R_Name", "R_Comment")
      .as[(String, String, String)]

    val nation = spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path = inputs(1))
      .toDF(colNames = "N_Nation_Key", "N_Name", "N_Region_Key", "N_Comment")
      .as[(String, String, String, String)]

    region.show()
    nation.show()

    val regionColumns = region.columns
    val nationColumns = nation.columns

    val test = region
      .flatMap(f => List(f._1, f._2, f._3) zip regionColumns)

    val test2 = nation
      .flatMap(f => List(f._1, f._2, f._3, f._4) zip nationColumns)

    test.union(test2).show(numRows = 200)

    /*val tables = spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(inputs: _*)

    tables.show(numRows = 30)
    tables.flatMap()*/

    // TODO
  }
}
