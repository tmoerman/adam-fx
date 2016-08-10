package org.tmoerman.adam.fx.snpeff

import SnpEffContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.scalatest.BeforeAndAfter
import org.tmoerman.adam.fx.BaseSparkContextSpec

import scala.reflect.io.File

/**
  * @author Thomas Moerman
  */
class ParquetStorageSpec extends BaseSparkContextSpec with BeforeAndAfter {

  val vcf     = "src/test/resources/small.snpEff.vcf"

  val out_vcf = "src/test/resources/small.snpEff.out.vcf"

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

  // TODO save as vcf

  "Saving AnnotatedGenotypes loaded from vcf back to vcf" should "work" in {
    val fromVCF = sc.loadAnnotatedGenotypes(vcf)

    fromVCF.map(_.getGenotype).toVariantContext().saveAsVcf(out_vcf, coalesceTo = Some(1))
  }

  "Saving AnnotatedGenotypes loaded from Parquet back to vcf" should "work" in {
    val fromVCF = sc.loadAnnotatedGenotypes(vcf)

    fromVCF.adamParquetSave(temp)

    val fromParquet = sc.loadAnnotatedGenotypes(temp)

    fromParquet.map(_.getGenotype).toVariantContext().saveAsVcf(out_vcf, coalesceTo = Some(1))
  }

  def deleteFiles() {
    File(temp).deleteRecursively()
    File(out_vcf).deleteRecursively()
  }

  before {
    deleteFiles()
  }

  after {
    deleteFiles()
  }

}