import java.util.Locale

import ConfigReader.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source

object ReadAndConvert extends App{
  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Data Transformer")
    .getOrCreate()
  val pathToConfig = "config\\conf.json"
  val conf: ConfigReader.Config = ConfigReader.readConfig(pathToConfig)
  spark.sparkContext.setLogLevel("ERROR")

  val operationsConf: Option[ConfigReader.Table] = conf.tables.toList.find(_.name == "Operation")
  val operationsSchema: StructType = CsvToParquetConverter.makeSchemaFromJson(operationsConf.get.schema)
  val operationsDf: DataFrame = spark.read
    .schema(operationsSchema)
    .parquet(s"data_files\\parquet\\${operationsConf.get.name}.parquet")
  operationsDf.show()

}
