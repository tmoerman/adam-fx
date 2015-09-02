package org.tmoerman.adam.fx.snpeff

import SnpEffContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.scalatest.BeforeAndAfter
import org.tmoerman.adam.fx.snpeff.model.VariantContextWithSnpEffAnnotations

import scala.reflect.io.File

/**
 * @author Thomas Moerman
 */
class ParquetStorageSpec extends BaseSparkContextSpec with BeforeAndAfter {

  val vcf     = "src/test/resources/small.snpEff.vcf"
  val parquet = "src/test/resources/small.snpEff.adam"
  val temp    = "src/test/temp/small.snpEff.adam"

  "SnpEffAnnotations saved to Parquet" should "match the original when loaded again" in {
    val fromFile = sc.loadSnpEffAnnotations(vcf)

    fromFile.adamParquetSave(temp)

    val fromParquet = sc.loadSnpEffAnnotations(temp)

    assert(fromFile.take(10) === fromParquet.take(10))
  }

  "Rich types loaded from vcf" should "match rich types loaded from Parquet" in {
    val fromFile    = sc.loadVariantsWithSnpEffAnnotations(vcf)
    val fromParquet = sc.loadVariantsWithSnpEffAnnotations(parquet)

    val projected = (v: VariantContextWithSnpEffAnnotations) => (v.variant, v.position, v.snpEffAnnotations)

    assert(fromFile.map(v => projected(v)).take(10) === fromParquet.map(v => projected(v)).take(10))
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