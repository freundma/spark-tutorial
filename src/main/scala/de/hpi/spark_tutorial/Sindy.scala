package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    def csv_to_df(path: String): DataFrame = {
      spark
        .read
        .options(Map("inferSchema" -> "true", "delimiter" -> ";", "header" -> "true"))
        .csv(path = path)
    }


    val region = csv_to_df(inputs.head)
    val nation = csv_to_df(inputs(1))
    val supplier = csv_to_df(inputs(2))
    val customer= csv_to_df(inputs(3))
    val part = csv_to_df(inputs(4))
    val lineitem = csv_to_df(inputs(5))
    val orders = csv_to_df(inputs(6))

    val regionFieldNames = region.schema.fieldNames
    val nationFieldNames = nation.schema.fieldNames
    val supplierFieldNames = supplier.schema.fieldNames
    val customerFieldNames = customer.schema.fieldNames
    val partFieldNames = part.schema.fieldNames
    val lineitemFieldNames = lineitem.schema.fieldNames
    val ordersFieldNames = orders.schema.fieldNames

    val regionFlat = region.as[(String, String, String)]
      .flatMap(f => List(f._1, f._2, f._3) zip regionFieldNames)

    val nationFlat = nation.as[(String, String, String, String)]
      .flatMap(f => List(f._1, f._2, f._3, f._4) zip nationFieldNames)

    val supplierFlat = supplier.as[(String, String, String, String, String, String, String)]
      .flatMap(f => List(f._1, f._2, f._3, f._4, f._5, f._6, f._7) zip supplierFieldNames)

    val customerFlat = customer.as[(String, String, String, String, String, String, String, String)]
      .flatMap(f => List(f._1, f._2, f._3, f._4, f._5, f._6, f._7, f._8) zip customerFieldNames)

    val partFlat = part.as[(String, String, String, String, String, String, String, String, String)]
      .flatMap(f => List(f._1, f._2, f._3, f._4, f._5, f._6, f._7, f._8, f._9) zip partFieldNames)
    nationFlat.show()

    val lineitemsFlat = lineitem.as[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]
      .flatMap(f => List(f._1, f._2, f._3, f._4, f._5, f._6, f._7, f._8, f._9, f._10, f._11, f._12, f._13, f._14, f._15, f._16) zip lineitemFieldNames)

    val ordersFlat = orders.as[(String, String, String, String, String, String, String, String, String)]
      .flatMap(f => List(f._1, f._2, f._3, f._4, f._5, f._6, f._7, f._8, f._9) zip ordersFieldNames)

    val attributeValuePairs = regionFlat.union(nationFlat).union(supplierFlat).union(customerFlat).union(partFlat)
      .union(lineitemsFlat).union(ordersFlat)

    val key_sets = attributeValuePairs
      .map(f => (f._1, Set(f._2))).rdd.reduceByKey((s1, s2) => s1.union(s2))
      .toDF().drop("_1").as[(List[String])]

    val inclusionLists = key_sets.
      flatMap(f => f.map(element => (element, f.filterNot(x => x == element))))

    val reducedInclusionList = inclusionLists.rdd.reduceByKey((s1, s2) => s1.intersect(s2)).toDF()
      .as[(String, List[String])].filter(x => x._2.nonEmpty).collect()

    reducedInclusionList.foreach(f => println(f._1 + " < " + f._2.toArray.mkString(", ")))

  }
}
