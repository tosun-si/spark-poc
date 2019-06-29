package org.tosunsi.sparkpoc.mapper

import org.apache.spark.sql.DataFrame

/**
 * Default trait for all the mappers.
 * Gives an abstract method to build columns for a specific domain.
 */
trait Mapper {

  /**
   * Build all the columns for a domain object. These columns are given in a [[DataFrame]].
   *
   * @return DataFrame a data frame with all the columns
   */
  def buildColumns(): DataFrame
}
