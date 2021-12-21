import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object CsvToParquetTransformer extends App {
  override def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("CSV to Parquet Transformer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read
      .option("multiline", "true")
      .json("data_files\\Json\\SchemaSample.json")
      .cache()
    df.printSchema()
    df.show(false)

    //Define custom schema
    val schemaFromJson = makeSchemaFromJson("data_files\\Json\\CoursesSchema.json")
    val df2 = spark.read.schema(schemaFromJson)
      .option("header", "true")
      .option("sep", ";")
      .csv("data_files\\csv\\Courses.csv")
      .cache()
    df2.printSchema()
    df2.show()
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