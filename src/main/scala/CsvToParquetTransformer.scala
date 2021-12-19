import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CsvToParquetTransformer extends App{
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("CSV to Parquet Transformer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.json("data_files\\Json\\config.json")
    df.printSchema()
    df.show(false)

//  def readConfig(pathToConfig: Path): Unit ={
//    val schema = StructType(Seq(
//      StructField("key", StringType, false),
//      StructField("value", DoubleType, false)
//    ))
//  }
}