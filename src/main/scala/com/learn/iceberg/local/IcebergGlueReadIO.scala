package com.learn.iceberg.local

import org.apache.spark.sql.SparkSession

object IcebergGlueReadIO {
  def main(args: Array[String]): Unit = {


    // Create a Spark session
    val spark = SparkSession.builder()
      //.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.6.1")
      .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
      .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.catalog.glue.warehouse", "iceberg_database")
      .config("spark.sql.catalog.glue.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.access.key", "<<S3_ACCESS_KEY>>")
      .config("spark.hadoop.fs.s3a.secret.key", "<<S3_SECRET_KEY>>")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") // Explicitly set endpoint for AWS S3
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      //.config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
      .config("spark.sql.defaultCatalog", "glue")
      .master("local[*]") // Running locally
      .getOrCreate()

    println("Fetching sample data for table: iceberg_database.employee")
    val sampleData = spark.sql("SELECT * FROM iceberg_database.employee LIMIT 5")
    sampleData.show() // Print the sample data
    println("end of sample data")

    spark.stop()
  }
}
