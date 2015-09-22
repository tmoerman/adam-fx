package org.tmoerman.adam.fx.snpeff

import SnpEffContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.scalatest.BeforeAndAfter

import scala.reflect.io.File

/**
 * @author Thomas Moerman
 */
class ParquetStorageSpec extends BaseSparkContextSpec with BeforeAndAfter {

  val vcf     = "src/test/resources/small.snpEff.vcf"

  val temp    = "src/test/temp/small.snpEff.adam"

  "AnnotatedVariants saved to Parquet" should "match the original when loaded again" in {
    val fromFile = sc.loadAnnotatedVariants(vcf)

    fromFile.adamParquetSave(temp)

    val fromParquet = sc.loadAnnotatedVariants(temp)

    assert(fromFile.take(10) === fromParquet.take(10))
  }

  "AnnotatedGenotypes saved to Parquet" should "match the original when loaded again" in {
    val fromFile = sc.loadAnnotatedGenotypes(vcf)

    fromFile.adamParquetSave(temp)

    val fromParquet = sc.loadAnnotatedGenotypes(temp)

    assert(fromFile.take(10) === fromParquet.take(10))
  }

  def deleteParquetFile() {
    File(temp).deleteRecursively()
  }

  before {
    deleteParquetFile()
  }

  after {
    deleteParquetFile()
  }

}