import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source


object ConfigReader {
  def main(args: Array[String]): Unit = {
    val conf = readConfig("config\\conf.json")

    for (tab <- conf.tables) {
      println(s"name: ${tab.name}\npath: ${tab.path}" +
        s"schema: ${tab.schema}\nseparator: ${tab.sep}\ndate-format: ${tab.dateFormat}")
    }
//    println(conf.tables)
  }

  /**
   * JSON-object for configuring the properties of the file to be converted to DF
   *
   * @param name   name of table
   * @param path   absolute path to file
   * @param schema path to schema definition file
   * @param sep    separator, used in file
   * @param dateFormat date format, used in file
   */
  case class Table(name: String, path: String, schema: String,
                   sep: String, dateFormat: String, partitionByField: List[String])

  /**
   * Config parsed from JSON-config
   *
   * @param tables list of tables
   */
  case class Config(pathToWriteParquet: String,
                    tables: List[Table])

  def readConfig(pathToConfig: String): Config = {
    implicit val formats: Formats = DefaultFormats // Brings in default date formats etc.

    val schemaSource = Source.fromFile(pathToConfig)
    val jsonString = schemaSource.getLines.mkString
    schemaSource.close()
    val jVal = parse(jsonString)
    jVal.extract[Config]
  }


}