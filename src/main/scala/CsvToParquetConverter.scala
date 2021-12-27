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
      val df = readCsvWithConfig(spark, table)
        .cache()
      // write df to parquet format
      writeDfToParquet(df, table, conf, SaveMode.Overwrite)
    }

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

  def readCsvWithConfig(spark: SparkSession,
                        tableConf: ConfigReader.Table,
                        headerOption: String = "true"): DataFrame = {
    //Read schema from file
    val schemaFromJson = makeSchemaFromJson(tableConf.schema)
    //create DF from csv
    spark.read.schema(schemaFromJson)
      .option("header", headerOption)
      .option("dateFormat", tableConf.dateFormat)
      .option("sep", tableConf.sep)
      .csv(tableConf.path)
  }

  def writeDfToParquet(df: DataFrame,
                       tableConf: ConfigReader.Table,
                       conf: ConfigReader.Config,
                       saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    val writeTo = if (tableConf.pathToWrite.isBlank) conf.defaultPathToWriteParquet else tableConf.pathToWrite
    df.write.partitionBy(tableConf.partitionByField: _*)
      .option("compression", tableConf.compressionMethod)
      .mode(saveMode)
      .parquet(s"$writeTo\\${tableConf.name}.parquet")
  }
}