package org.tosunsi.sparkpoc.mapper.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.tosunsi.sparkpoc.model.School

/**
 * Garbage class that gives all the UDFs for the Address domain.
 */
object AddressUdfs {

  val citySiteUdf = udf(toCitySiteLabel _)
  val addressLabelUdf = udf(toAddressLabel _)
  val evtNameUdf = udf(toEvtName _)
  val schoolsUdf = udf(getSchools _)
  val biggestSchoolUdf = udf(toBiggestSchool _)

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
