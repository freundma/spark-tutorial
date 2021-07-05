package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    val region = spark
      .read
      .options(Map("inferSchema"->"true","delimiter"->";", "header"-> "true"))
      .csv(path = inputs.head)
      //.toDF(colNames = "R_Region_Key", "R_Name", "R_Comment")
      //.as[(String, String, String)]

    val nation = spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path = inputs(1))
      .toDF(colNames = "N_Nation_Key", "N_Name", "N_Region_Key", "N_Comment")
      .as[(String, String, String, String)]

    val supplier = spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("delimiter",";")
      .csv(path = inputs(2))
      .toDF(colNames="S_SUPPKEY","S_NAME","S_ADDRESS","S_NATIONKEY","S_PHONE","S_ACCTBAL","S_COMMENT")
      .as[(String, String, String, String, String, String, String)]

    val nationColumns = nation.columns
    val supplierColumns = supplier.columns

    val regionFlat = region.flatMap(row => row.toSeq.toList zip region.schema.fieldNames)

    val nationFlat = nation
      .flatMap(f => f.productIterator.toList zip nationColumns)

    val supplierFlat = supplier
      .flatMap(f => List(f._1, f._2, f._3, f._4, f._5, f._6, f._7) zip supplierColumns)

    nationFlat.show()

    val attributeValuePairs = regionFlat.as[(String, String)].union(nationFlat).union(supplierFlat)

    val key_sets = attributeValuePairs
      .map(f => (f._1, Set(f._2))).rdd.reduceByKey((s1, s2) => s1.union(s2))
      .toDF().drop("_1").as[(List[String])]

    val inclusionLists = key_sets.
      flatMap(f => f.map(element => (element, f.filterNot(x => x == element))))

    val reducedInclusionList = inclusionLists.rdd.reduceByKey((s1, s2) => s1.intersect(s2)).toDF()
      .as[(String, List[String])].filter(x => x._2.nonEmpty).collect()

    reducedInclusionList.foreach(f => println(f._1 + " < " + f._2.toArray.mkString(", ")))


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
