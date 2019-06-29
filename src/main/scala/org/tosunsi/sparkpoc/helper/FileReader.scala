package org.tosunsi.sparkpoc.helper

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Gives helper methods for files reading.
 */
object FileReader {

  /**
   * Reads a CSV file from the given spark session and a file path.
   * The result is given as a [[DataFrame]].
   */
  def readCsv(sparkSession: SparkSession, filePath: String): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ";")
      .csv(filePath)
  }
}
