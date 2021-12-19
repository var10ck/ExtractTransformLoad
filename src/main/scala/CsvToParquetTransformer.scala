import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object CsvToParquetTransformer extends App{
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("CSV to Parquet Transformer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read
      .option("multiline","true")
      .json("F:\\scala_projects\\ExtractTransformLoad\\data_files\\Json\\schema_sample.json")
      .cache()
    df.printSchema()
    df.show(false)

    //Define custom schema
    val filePath = Source.fromFile("F:\\scala_projects\\ExtractTransformLoad\\data_files\\Json\\config.json")
    val bufferedSource = filePath
    val schemaSource = bufferedSource.getLines.mkString
    bufferedSource.close()
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    val df2 = spark.read.schema(schemaFromJson).csv("F:\\scala_projects\\ExtractTransformLoad\\data_files\\csv\\Courses.csv")
    df2.printSchema()

//  def readConfig(pathToConfig: Path): Unit ={
//    val schema = StructType(Seq(
//      StructField("key", StringType, false),
//      StructField("value", DoubleType, false)
//    ))
//  }
}