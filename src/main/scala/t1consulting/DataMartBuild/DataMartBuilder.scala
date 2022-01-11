package t1consulting.DataMartBuild

import org.apache.spark.sql.SparkSession

trait DataMartBuilder {
  val spark: SparkSession
  val ConfigDefaultPath: String
  val PathToWriteDM: String

}
