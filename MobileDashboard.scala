package com.hashmap1
import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row
import org.apache.spark.sql.{DataFrame, SparkSession, types}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}

object MobileDashboard {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._
    val customschema = StructType(
      List(
        StructField("S_No", IntegerType)
        , StructField("Child_Account_No", IntegerType)
        , StructField("AccNumber", IntegerType)
        , StructField("PhoneNumber", DataTypes.StringType)
        , StructField("User_Name", DataTypes.StringType)
        , StructField("Credit_Limit", IntegerType)
        , StructField("Monthly_Charges", DoubleType)
        , StructField("One_time_Charges", DoubleType)
        , StructField("Usage_Charges", DoubleType)
        , StructField("Discount_Rebates", DoubleType)
        , StructField("Current_Charges", DoubleType)
      ))
    var dfempty:DataFrame=spark.createDataFrame(spark.sparkContext.emptyRDD[Row], customschema)
   var df:DataFrame = dfempty.select((dfempty.col("PhoneNumber")).as("Phone_Number"), col("AccNumber"), col("Monthly_Charges"), col("Discount_Rebates"), col("Usage_Charges"), col("One_time_Charges").as("Other_Charges"), col("Current_Charges"))

    var dffinal:DataFrame=df.withColumn("Value_Added_Services", lit("null")).withColumn("Provider", lit("null")).withColumn("Month",lit("null"))

    val list:List[DataFrame]=List(dffinal)
    val filelist = getListOfFiles("/user/raj_ops/CelcomData")
    for (i <- filelist.indices) {


      val myfile: DataFrame = spark.sqlContext.read
        .format("com.crealytics.spark.excel")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "false")
        .option("skipFirstRows", 15)
        .option("startColumn", 0)
        .option("endColumn", 12)
        .schema(customschema)
        .load(filelist(i))

       val file1=myfile
        .filter(col("AccNumber").isNotNull === true)


      val extractPhoneNumber = udf((columnname: String) => {
        if (columnname != null && columnname.contains("-")) {
          if (columnname.length() == 11) {
            val extract = columnname.split("-")
            extract(1)
          }
          else {
            val extract1 = columnname.split("-")
            val extract = extract1(1).split("@")
            extract(0)
          }
        }
        else
          null
      })


      val transformedDF = file1.select(extractPhoneNumber(myfile.col("PhoneNumber")).as("Phone_Number"), col("AccNumber"), col("Monthly_Charges"), col("Discount_Rebates"), col("Usage_Charges"), col("One_time_Charges").as("Other_Charges"), col("Current_Charges").as("Total"))

      val transformedDF1 = transformedDF.withColumn("Value_Added_Services", lit("0")).withColumn("Provider", lit("Celcom"))


      val myfile1: DataFrame = spark.sqlContext.read
        .format("com.crealytics.spark.excel")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "false")
        .option("startColumn", 0)
        .option("endColumn", 12)
        .load(filelist(i))


      val date = myfile1.select(col("_c2")).take(5).drop(4).toList

      val dd = date(0).toString().replace("[", "").replace("]", "").split(" ")(1)

      val transformedDF2 = transformedDF1.withColumn("Month", lit(dd))

      dffinal=transformedDF2.union(dffinal)

    }
    dffinal.write.format("orc").mode("append").saveAsTable("demo.tbl_celcom_Data")
  }

  def getListOfFiles(path: String): List[String] = {
    val uri = new URI("hdfs://172.17.0.2:8020")
    val fs = FileSystem.get(uri, new Configuration())
    val filePath = new Path(path)
    val files = fs.listFiles(filePath, false)
    var fileList = List[String]()

    while (files.hasNext) {
      fileList ::= files.next().getPath.toString
    }
    fileList
  }

}
