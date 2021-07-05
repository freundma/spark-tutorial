package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    def csv_to_rdd(path: String): DataFrame = {
      spark
        .read
        .options(Map("inferSchema" -> "true", "delimiter" -> ";", "header" -> "true"))
        .csv(path = path)
    }

    val region = csv_to_rdd(inputs.head)
    val nation = csv_to_rdd(inputs(1))
    val supplier = csv_to_rdd(inputs(2))

    val regionFlat = region.as[(String, String, String)].
      flatMap(f => f.productIterator.asInstanceOf[Iterator[String]].toList zip region.schema.fieldNames)

    val nationFlat = nation.as[(String, String, String, String)]
      .flatMap(f => f.productIterator.asInstanceOf[Iterator[String]].toList zip nation.schema.fieldNames)

    val supplierFlat = supplier.as[(String, String, String, String, String, String, String)]
      .flatMap(f => f.productIterator.asInstanceOf[Iterator[String]].toList zip supplier.schema.fieldNames)

    nationFlat.show()

    val attributeValuePairs = regionFlat.union(nationFlat).union(supplierFlat)

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
