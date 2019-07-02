package org.tosunsi.sparkpoc.mapper.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.tosunsi.sparkpoc.model.School

/**
 * Garbage class that gives all the UDFs for the Address domain.
 */
object AddressUdfs {

  private val addressLabelMapper: (String, String) => String = toAddressLabel
  private val evtNameMapper: (String, String) => String = toEvtName
  private val citySiteMapper: (String, String) => String = toCitySiteLabel
  private val schoolsSupplier: () => Seq[School] = getSchools
  private val biggestSchoolMapper: Seq[Row] => School = toBiggestSchool

  val citySiteUdf = udf(citySiteMapper)
  val addressLabelUdf = udf(addressLabelMapper)
  val evtNameUdf = udf(evtNameMapper)
  val schoolsUdf = udf(schoolsSupplier)
  val biggerSchoolUdf = udf(biggestSchoolMapper)

  private def toAddressLabel(street: String, zipCode: String): String = {
    s"$street $zipCode"
  }

  private def toEvtName(evtName: String, oldEvtName: String): String = {
    s"$evtName $oldEvtName"
  }

  private def toCitySiteLabel(cityName: String, cityCode: String): String = {
    s"$cityName $cityCode"
  }

  private def getSchools(): Seq[School] = {
    Seq(
      School("Spider school", 256),
      School("Sayen school", 540),
      School("Ninja school", 369)
    )
  }

  private def toBiggestSchool(schools: Seq[Row]): School = {
    schools
      .map(toSchool)
      .maxBy(_.studentsNb)
  }

  private def toSchool(schoolRow: Row): School = {
    School(
      schoolRow.getAs(0),
      schoolRow.getAs(1)
    )
  }
}
