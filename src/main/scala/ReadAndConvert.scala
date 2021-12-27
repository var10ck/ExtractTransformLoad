import java.util.Locale

import ConfigReader.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source
import scala.reflect.io.Path

object ReadAndConvert extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Data Transformer")
    .getOrCreate()
  val pathToConfig = "config\\conf.json"
  val conf: ConfigReader.Config = ConfigReader.readConfig(pathToConfig)
  spark.sparkContext.setLogLevel("ERROR")

  val operationDf: DataFrame = createDfFromConf(conf, "Operation")
  val accountDf: DataFrame = createDfFromConf(conf, "Account")
  val rateDf: DataFrame = createDfFromConf(conf, "Rate")
  val clientDf: DataFrame = createDfFromConf(conf, "Client")

  operationDf.show()
  accountDf.show()
  rateDf.show()
  clientDf.show()

  /**
   * creating DataFrame, using parameters from Config object.
   *
   * @param config    instance of Config case-class, defined in ConfigReader object
   * @param tableName name of table, which parameters will be read from config
   * @return DataFrame of table with tableName
   */
  def createDfFromConf(config: ConfigReader.Config, tableName: String): DataFrame = {
    val tableConf: Option[ConfigReader.Table] = config.tables.find(_.name == tableName)
    val tableSchema: StructType = CsvToParquetConverter.makeSchemaFromJson(tableConf.get.schema)
    spark.read
      .schema(tableSchema)
      .parquet(s"${config.defaultPathToWriteParquet}/${tableConf.get.name}.parquet")
  }

  /**
   * creating DataFrame, using parameters from config file. Using default path to config-file.
   *
   * @param tableName name of table, which parameters will be read from config
   * @return DataFrame of table with tableName
   */
  def createDfFromConf(tableName: String): DataFrame = {
    val config = ConfigReader.readConfig("config\\conf.json")
    createDfFromConf(config, tableName)
  }

}
