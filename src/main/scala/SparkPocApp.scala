import org.apache.spark.sql.SparkSession
import org.tosunsi.sparkpoc.mapper.AddressMapper

object SparkPocApp {
  def main(args: Array[String]) {
    implicit val sparkSession = SparkSession
      .builder
      .appName("Spark POC app")
      .master("local[*]")
      .getOrCreate()

    new AddressMapper()
      .buildColumns()
      .show()
  }
}
