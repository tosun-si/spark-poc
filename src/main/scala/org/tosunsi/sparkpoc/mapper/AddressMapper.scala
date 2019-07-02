package org.tosunsi.sparkpoc.mapper

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tosunsi.sparkpoc.helper.FileReader
import org.tosunsi.sparkpoc.mapper.udf.AddressUdfs._
import org.tosunsi.sparkpoc.model.AddressColumns._

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
      .withColumn(BIGGER_SCHOOL, biggerSchoolUdf(col(SCHOOLS)))
      .addSiteCity()
  }

  implicit class CitySiteMapper(df: DataFrame) {

    def addSiteCity(): DataFrame = {
      df.withColumn(CITY_SITE, citySiteUdf(col(ADDRESS_STREET_TYPE), col(SITE_CITIES)))
    }
  }

}
