package t1consulting

import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object CalculationParamsTechCreator extends App {
  val paramsString_1: String = "%а/м%, %а\\\\м%, %автомобиль %, %автомобили %, %транспорт%, %трансп%средс%, %легков%, " +
    "%тягач%, %вин%, %vin%,%viн:%, %fоrd%, %форд%,%кiа%, %кия%, %киа%%мiтsuвisнi%," +
    " %мицубиси%, %нissан%, %ниссан%, %sсанiа%, %вмw%, %бмв%, %аudi%, %ауди%, %jеер%, %джип%, %vоlvо%, %вольво%," +
    " %тоyота%, %тойота%, %тоиота%, %нyuнdаi%, %хендай%, %rенаulт%, %рено%, %реugеот%, %пежо%, %lаdа%, %лада%," +
    " %dатsuн%, %додж%, %меrсеdеs%, %мерседес%, %vоlкswаgен%, %фольксваген%, %sкоdа%, %шкода%, %самосвал%, %rover%, %ровер%"

  val paramsString_2: String = "% сою%, %соя%, %зерно%, %кукуруз%, %масло%, %молок%, %молоч%, %мясн%, %мясо%, %овощ%," +
    " %подсолн%, %пшениц%, %рис%, %с/х%прод%, %с/х%товар%, %с\\\\х%прод%, %с\\\\х%товар%, %сахар%, %сельск%прод%, " +
    "%сельск%товар%, %сельхоз%прод%, %сельхоз%товар%, %семен%, %семечк%, %сено%, %соев%, %фрукт%, %яиц%, %ячмен%, " +
    "%картоф%, %томат%, %говя%, %свин%, %курин%, %куриц%, %рыб%, %алко%, %чаи%, %кофе%, %чипс%, %напит%, %бакале%, " +
    "%конфет%, %колбас%, %морож%, %с/м%, %с\\\\м%, %консерв%, %пищев%, %питан%, %сыр%, %макарон%, %лосос%, %треск%, " +
    "%саир%, % филе%, % хек%, %хлеб%, %какао%, %кондитер%, %пиво%, %ликер%"
  val paramsList_1: Array[String] = paramsString_1.split(",").map(_.strip)
  val paramsList_2: Array[String] = paramsString_2.split(",").map(_.strip)

  val schema: StructType = StructType(
    Array(
      StructField("list_num", IntegerType, nullable = false),
      StructField("mask_string", StringType, nullable = true)
    )
  )
    println(paramsList_1.foldLeft("list 1:\n")(_ + " " + _))
    println(paramsList_2.foldLeft("list 2:\n")(_ + " " + _))
  var paramListRows: ListBuffer[Row] = ListBuffer[Row]()
  for (p <- paramsList_1) {
    paramListRows.append(Row(1, p))
  }
  for (p <- paramsList_2) {
    paramListRows += Row(2, p)
  }
  //  paramListRows.foreach(println)

  val spark: SparkSession = SparkSession.
    builder()
    .master("local[4]")
    .appName("calculation_params_tech creator")
    .getOrCreate()
  val rdd = spark.sparkContext.parallelize(paramListRows)
  val df: DataFrame = spark.createDataFrame(rdd, schema)
    .withColumn("id", monotonically_increasing_id())
//  df.show(100)
  df.write
    .partitionBy("list_num")
    .mode(SaveMode.Overwrite)
    .parquet("data_files\\parquet\\calculation_params_tech.parquet")

  spark.read.parquet("data_files\\parquet\\calculation_params_tech.parquet").show()
}
