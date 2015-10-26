package org.tmoerman.adam.fx.filter

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Genotype

/**
 * @author Thomas Moerman
 */
class FilterByRegionFunctions(rdd: RDD[Genotype]) extends Serializable {

  def filterByOverlappingRegions(query: Seq[ReferenceRegion]): RDD[Genotype] = {

    def predicate(genotype: Genotype): Boolean = {

      //query.exists()
      false

    }

    null

  }

}
