package com.learn.iceberg.local

import org.apache.spark.sql.SparkSession

object IcebergLocalReadIO {
  def main(args: Array[String]): Unit = {

    val projectDir = System.getProperty("user.dir")
    println(s"Current project directory: $projectDir")

    // Create a Spark session
    val warehousePath = s"file://$projectDir/data"
    val spark = SparkSession.builder()
      .appName("Iceberg Local IO")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", warehousePath)
      .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local_catalog.type", "hadoop")
      .config("spark.sql.catalog.local_catalog.warehouse", warehousePath)
      .getOrCreate()

    // Define the table name and path
    val tableName = "local_catalog.db.sample_table"
    val tablePath = s"$warehousePath/db/sample_table"

    // Read data from Iceberg table using path
        val readDfPath = spark.read.format("iceberg").load(tableName)
        println("Data read from Iceberg table using path:")
        readDfPath.show()

    spark.stop()
  }
}
