import java.util.Locale

import ConfigReader.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source


object CsvToParquetConverter extends App {
  override def main(args: Array[String]): Unit = {
    //    Locale.setDefault(Locale.ENGLISH)
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("CSV to Parquet Transformer")
      .getOrCreate()
    //reading config
    val pathToConfig = "config\\conf.json"
    val conf: ConfigReader.Config = ConfigReader.readConfig(pathToConfig)

    spark.sparkContext.setLogLevel("ERROR")
    
    for (table <- conf.tables) {
      //Read schema from file
      val schemaFromJson = makeSchemaFromJson(table.schema)
      //create DF from csv
      val df = spark.read.schema(schemaFromJson)
        .option("header", "true")
        .option("dateFormat", table.dateFormat)
        .option("sep", table.sep)
        .csv(table.path)
        .cache()
//      df.printSchema()
//      df.show()
      df.write.partitionBy(table.partitionByField:_*)
        .option("compression", "none")
        .mode(SaveMode.Overwrite)
        .parquet(s"data_files\\parquet\\${table.name}.parquet")
    }
    //    //Read schema from file
    //    val schemaFromJson = makeSchemaFromJson("data_files\\json\\AccountsSchema.json")
    //    //create DF from csv
    ////
    //    val df = spark.read.schema(schemaFromJson)
    //      .option("header", "true")
    //      .option("dateFormat", "yyyy-MM-dd")
    //      .option("sep", ";")
    //      .csv("data_files\\csv\\Accounts.csv")
    //      .cache()
    //    df.printSchema()
    //    df.show()


    //    df.write.partitionBy("RateDate")
    //      .option("compression", "none")
    //      .mode(SaveMode.Overwrite)
    //    //      .parquet("data_files\\parquet\\Courses.parquet")
    //
    //    val parqDF = spark.read.parquet("data_files\\parquet\\Courses.parquet")
    //    parqDF.createOrReplaceTempView("Courses")
    //    val sparkSQLRequest = spark.sql("select * from Courses")
    //    sparkSQLRequest.explain()
    //    sparkSQLRequest.show()
    //    sparkSQLRequest.printSchema()
    //    df.show()

  }

  /**
   * Parsing schema from JSON file
   *
   * @param pathToJson path to JSON file with schema definition
   * @return Schema, parsed from given file
   **/
  def makeSchemaFromJson(pathToJson: String): StructType = {
    val schemaSource = Source.fromFile(pathToJson)
    val jsonString = schemaSource.getLines.mkString
    schemaSource.close()
    DataType.fromJson(jsonString).asInstanceOf[StructType]
  }

}