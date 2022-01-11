package t1consulting.DataMartBuild

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.Source
import t1consulting._

object DataMartSqlBuilder extends App {
  final val ConfigDefaultPath = "config\\conf.json"
  final val CalculationParamsTechPath = "data_files\\parquet\\calculation_params_tech.parquet"
  final val PathToWriteCorporatePaymentsDM = "data_files/DataMarts/corporate_payments.parquet"
  final val PathToWriteCorporateAccountDM = "data_files/DataMarts/corporate_account.parquet"
  final val PathToWriteCorporateInfoDM = "data_files/DataMarts/corporate_info.parquet"

  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Data Transformer")
    .getOrCreate()
  val pathToConfig = "config\\conf.json"
  val conf: ConfigReader.Config = ConfigReader.readConfig(pathToConfig)
  spark.sparkContext.setLogLevel("ERROR")

  // creating Data frames and temp views of tables
  val operationDf: DataFrame = createDfFromConf(spark, conf, "Operation")
  val accountDf: DataFrame = createDfFromConf(spark, conf, "Account")
  val rateDf: DataFrame = createDfFromConf(spark, conf, "Rate")
  val clientDf: DataFrame = createDfFromConf(spark, conf, "Client")
  val calculationParamsTechDf = spark.read.parquet(CalculationParamsTechPath)
  operationDf.createOrReplaceTempView("Operation")
  accountDf.createOrReplaceTempView("Account")
  rateDf.createOrReplaceTempView("Rate")
  clientDf.createOrReplaceTempView("Client")
  calculationParamsTechDf.createOrReplaceTempView("calculation_params_tech")

  // building corporate_payments data mart
  val CorpPaymentsDf = createDfFromSQL(spark, "src\\main\\sql_queries\\CorporatePaymentsDM.sql")
  CorpPaymentsDf.explain()
  spark.time(CorpPaymentsDf.show(50))
  CorpPaymentsDf.printSchema()
  CorpPaymentsDf.createOrReplaceTempView("corporate_payments")

  // building corporate_account data mart
  val CorpAccountDf = createDfFromSQL(spark, "src\\main\\sql_queries\\CorporateAccountDM.sql")
  CorpAccountDf.explain()
  spark.time(CorpAccountDf.show(50))
  CorpAccountDf.printSchema()
  CorpAccountDf.createOrReplaceTempView("corporate_account")

  // building corporate_info data mart
  val CorpInfoDf = createDfFromSQL(spark, "src\\main\\sql_queries\\CorporateInfoDM.sql")
  CorpInfoDf.explain()
  spark.time(CorpInfoDf.show(50))
  CorpInfoDf.printSchema()
  //  CorpAccountDf.createOrReplaceTempView("corporate_account")

  //  CorpPaymentsDf.write
  //    .partitionBy("CutoffDate")
  //    .mode(SaveMode.Overwrite)
  //    .parquet(PathToWriteCorporatePaymentsDM)
  //  CorpAccountDf.write
  //    .partitionBy("CutoffDate")
  //    .mode(SaveMode.Overwrite)
  //    .parquet(PathToWriteCorporateAccountDM)
  CorpInfoDf.write
    .partitionBy("CutoffDate")
    .mode(SaveMode.Overwrite)
    .parquet(PathToWriteCorporateAccountDM)

  /**
   * creating DataFrame, using parameters from Config object.
   *
   * @param spark     SparkSession
   * @param config    instance of Config case-class, defined in ConfigReader object
   * @param tableName name of table, which parameters will be read from config
   * @return DataFrame of table with tableName
   */
  def createDfFromConf(spark: SparkSession, config: ConfigReader.Config, tableName: String): DataFrame = {
    val tableConf: Option[ConfigReader.Table] = config.tables.find(_.name == tableName)
    val tableSchema: StructType = CsvToParquetConverter.makeSchemaFromJson(tableConf.get.schema)
    spark.read
      .schema(tableSchema)
      .parquet(s"${config.defaultPathToWriteParquet}/${tableConf.get.name}.parquet")
  }

  /**
   * creating DataFrame, using parameters from config file. Using default path to config-file.
   *
   * @param spark     SparkSession
   * @param tableName name of table, which parameters will be read from config
   * @return DataFrame of table with tableName
   */
  def createDfFromConf(spark: SparkSession, tableName: String): DataFrame = {
    val config = ConfigReader.readConfig(ConfigDefaultPath)
    createDfFromConf(spark, config, tableName)
  }

  def readSqlFromFile(path: String): String = {
    val src = Source.fromFile(path)
    val sqlString = src.getLines.mkString("\n")
    src.close()
    sqlString
  }

  def createDfFromSQL(spark: SparkSession, pathToSql: String) =
    spark.sql(readSqlFromFile(pathToSql))
}
