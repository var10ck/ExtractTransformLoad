import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._

import scala.io.Source

object CsvToParquetTransformer extends App {
  override def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("CSV to Parquet Transformer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //Read schema from file
    val schemaFromJson = makeSchemaFromJson("data_files\\Json\\CoursesSchema.json")
    //create DF from csv
    val df = spark.read.schema(schemaFromJson)
      .option("header", "true")
      .option("sep", ";")
      .csv("data_files\\csv\\Courses.csv")
      .cache()
    df.printSchema()
    df.show()

    df.write.partitionBy("RateDate")
      .option("compression", "none")
      .mode(SaveMode.Overwrite)
      .parquet("data_files\\parquet\\Courses.parquet")

//    val parqDF = spark.read.parquet("data_files\\parquet\\Courses.parquet")
//    parqDF.createOrReplaceTempView("Courses")
//    val sparkSQLRequest = spark.sql("select * from Courses")
//    sparkSQLRequest.explain()
//    sparkSQLRequest.show()
//    sparkSQLRequest.printSchema()


  }

  /**
   * Parsing schema from JSON file
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