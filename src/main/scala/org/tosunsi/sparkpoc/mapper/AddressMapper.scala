package org.tosunsi.sparkpoc.mapper

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tosunsi.sparkpoc.helper.FileReader
import org.tosunsi.sparkpoc.mapper.AddressMapper._
import org.tosunsi.sparkpoc.model.AddressColumns._
import org.tosunsi.sparkpoc.model.School

/**
 * Mapper for an address.
 */
class AddressMapper(implicit sparkSession: SparkSession) extends Mapper {

  override def buildColumns(): DataFrame = {
    val csvFilePath = "src/main/resources/files/espaces_verts.csv"

    FileReader.readCsv(sparkSession, csvFilePath)
      .withColumn(LABEL, addressLabelUdf(col(ADDRESS_STREET_LABEL), col(ADDRESS_ZIP_CODE)))
      .withColumn(EVT_NAME, evtNameUdf(col(EV_NAME), col(OLD_EV_NAME)))
      .withColumn(COUNTRY, lit("France"))
      .withColumn(SCHOOLS, schoolsUdf())
      .addSiteCity()
  }

  implicit class CitySiteMapper(df: DataFrame) {

    def addSiteCity(): DataFrame = {
      df.withColumn(CITY_SITE, citySiteUdf(col(ADDRESS_STREET_TYPE), col(SITE_CITIES)))
    }
  }

}

object AddressMapper {

  private val addressLabelMapper: (String, String) => String = toAddressLabel
  private val evtNameMapper: (String, String) => String = toEvtName
  private val citySiteMapper: (String, String) => String = toCitySiteLabel
  private val schoolsSupplier: () => List[School] = getSchools

  private val citySiteUdf = udf(citySiteMapper)
  private val addressLabelUdf = udf(addressLabelMapper)
  private val evtNameUdf = udf(evtNameMapper)
  private val schoolsUdf = udf(schoolsSupplier)

  private def toAddressLabel(street: String, zipCode: String): String = {
    s"$street $zipCode"
  }

  private def toEvtName(evtName: String, oldEvtName: String): String = {
    s"$evtName $oldEvtName"
  }

  private def toCitySiteLabel(cityName: String, cityCode: String): String = {
    s"$cityName $cityCode"
  }

  private def getSchools(): List[School] = {
    List(
      School("Spider school"),
      School("Sayen school"),
      School("Ninja school")
    )
  }
}
