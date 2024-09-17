package com.learn.iceberg.local

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object IcebergLocalReadWriteIO {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val projectDir = System.getProperty("user.dir")
    println(s"Current project directory: $projectDir")
    val warehousePath = s"file://$projectDir/data"

    val spark = SparkSession.builder()
      .appName("Iceberg Local IO")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", warehousePath)
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehousePath)
      .getOrCreate()

    import spark.implicits._

    // Define the table name and path
    val tableName = "local.db.sample_table"
    val tablePath = s"$warehousePath/db/sample_table"

    // Create sample data
    val data = Seq(
      (1, "Alice", 30),
      (2, "Bob", 32),
      (3, "Charlie", 28)
    ).toDF("id", "name", "age")

//     Write data to Iceberg table
    data.writeTo(tableName)
      .using("iceberg")
      .createOrReplace()

    println("Data written to Iceberg table")

    // Read data from Iceberg table using table name
    val readDf = spark.table(tableName)
    println("Data read from Iceberg table using table name:")
    readDf.show()

    // Read data from Iceberg table using path
//    val readDfPath = spark.read.format("iceberg").load(tablePath)
//    println("Data read from Iceberg table using path:")
//    readDfPath.show()

    // Perform an update
    spark.sql(s"UPDATE $tableName SET age = age + 300 WHERE name = 'Charlie'")
    println("After update:")
    spark.table(tableName).show()

    // Read a specific snapshot (replace with actual snapshot ID after running once)
//    val snapshotId = spark.table(tableName).snapshotId
//    println(s"Current snapshot ID: $snapshotId")

//    val snapshotDf = spark.read
//      .option("snapshot-id", snapshotId)
//      .format("iceberg")
//      .load(tablePath)
//    println("Data from specific snapshot:")
//    snapshotDf.show()

    // Stop the Spark session
    spark.stop()
  }
}
