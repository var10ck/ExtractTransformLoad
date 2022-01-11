package t1consulting

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object test extends App{
  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Data Transformer")
    .getOrCreate()
  val pathToConfig = "config\\conf.json"
  val conf: ConfigReader.Config = ConfigReader.readConfig(pathToConfig)
  spark.sparkContext.setLogLevel("ERROR")

  val accountsConf: Option[ConfigReader.Table] = conf.tables.find(_.name == "Account")
  val accountsSchema: StructType = CsvToParquetConverter.makeSchemaFromJson(accountsConf.get.schema)
  val accountsDf: DataFrame = spark.read
    .schema(accountsSchema)
    .parquet(s"data_files\\parquet\\${accountsConf.get.name}.parquet")
  accountsDf.printSchema()
  accountsDf.show()

}
