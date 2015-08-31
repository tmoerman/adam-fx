package org.tmoerman.adam.fx.snpeff

import org.bdgenomics.adam.rdd.ADAMContext._
import org.scalatest.BeforeAndAfter

import scala.reflect.io.File

/**
 * @author Thomas Moerman
 */
class ParquetStorageSpec extends BaseSnpEffContextSpec with BeforeAndAfter {

  val vcf = "src/test/resources/small.snpEff.vcf"

  val parquet = "src/test/temp/small.snpEff.adam"

  "SnpEffAnnotations loaded from file" should "match SnpEffAnnotations loaded from Parquet" in {

    val annotationsFromFile = ec.loadSnpEffAnnotations(vcf)

    annotationsFromFile.adamParquetSave(parquet)

    val annotationsFromParquet = ec.loadSnpEffAnnotations(parquet)

    assert(annotationsFromFile.take(10) === annotationsFromParquet.take(10))
  }

  def deleteParquetFile() {
    File(parquet).deleteRecursively()
  }

  before {
    deleteParquetFile()
  }

  after {
    deleteParquetFile()
  }

}