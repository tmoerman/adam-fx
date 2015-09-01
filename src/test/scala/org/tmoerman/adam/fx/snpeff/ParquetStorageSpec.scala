package org.tmoerman.adam.fx.snpeff

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Variant
import org.scalatest.BeforeAndAfter
import org.tmoerman.adam.fx.avro.SnpEffAnnotations
import org.tmoerman.adam.fx.snpeff.model.VariantContextWithSnpEffAnnotations

import scala.reflect.io.File

/**
 * @author Thomas Moerman
 */
class ParquetStorageSpec extends BaseSnpEffContextSpec with BeforeAndAfter {

  val vcf     = "src/test/resources/small.snpEff.vcf"
  val parquet = "src/test/resources/small.snpEff.adam"
  val temp    = "src/test/temp/small.snpEff.adam"

  "SnpEffAnnotations saved to Parquet" should "match the original when loaded again" in {
    val fromFile = ec.loadSnpEffAnnotations(vcf)

    fromFile.adamParquetSave(temp)

    val fromParquet = ec.loadSnpEffAnnotations(temp)

    assert(fromFile.take(10) === fromParquet.take(10))
  }

  "Rich types loaded from vcf" should "match rich types loaded from Parquet" in {
    val fromFile    = ec.loadVariantsWithSnpEffAnnotations(vcf)
    val fromParquet = ec.loadVariantsWithSnpEffAnnotations(parquet)

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