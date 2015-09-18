package org.tmoerman.adam.fx.snpeff

import SnpEffContext._
import scala.collection.JavaConverters._

/**
 * @author Thomas Moerman
 */
class LoadSnpSiftCasesSpec extends BaseSparkContextSpec {

  val dbSnpCases = "src/test/resources/cases.snpSift.vcf"

  val all = sc.loadVariantsWithSnpEffAnnotations(dbSnpCases).collect().toList.toArray

  val (one, two, three, four, five, six, seven) = all match { case Array(a, b, c, d, e, f, g) => (a, b, c, d, e, f, g) }

  "one and two" should "not have dbSnpAnnotations" in {
    one.snpEffAnnotations.get.dbSnpAnnotations shouldBe empty
    two.snpEffAnnotations.get.dbSnpAnnotations shouldBe empty
  }

  "one to four" should "not have clinvarAnnotations" in {
    one.snpEffAnnotations.get.clinvarAnnotations   shouldBe empty
    two.snpEffAnnotations.get.clinvarAnnotations   shouldBe empty
    three.snpEffAnnotations.get.clinvarAnnotations shouldBe empty
    four.snpEffAnnotations.get.clinvarAnnotations  shouldBe empty
  }

  "three" should "have dbSnpAnnotations with correct properties" in {
    val dbSnpAnnotations3 = three.snpEffAnnotations.get.dbSnpAnnotations.get

    dbSnpAnnotations3.getRS  shouldBe List(6682385, 75454623).asJava
    dbSnpAnnotations3.getVLD shouldBe true
    dbSnpAnnotations3.getOM  shouldBe false
  }

  "four" should "have dbSnpAnnotations with correct properties" in {
    val dbSnpAnnotations4 = four.snpEffAnnotations.get.dbSnpAnnotations.get

    dbSnpAnnotations4.getRS  shouldBe List(11586607).asJava
    dbSnpAnnotations4.getVLD shouldBe true
    dbSnpAnnotations4.getOM  shouldBe false
  }

  "five" should "have clinvarAnnotations with correct properties" in {
    val clinvarAnnotations5 = five.snpEffAnnotations.get.clinvarAnnotations.get

    clinvarAnnotations5.getCLNDSDBID shouldBe List("CN169374").asJava
  }

  "six" should "have clinvarAnnotations with correct properties" in {
    val clinvarAnnotations6 = six.snpEffAnnotations.get.clinvarAnnotations.get

    clinvarAnnotations6.getCLNDSDBID shouldBe List("C0027672:699346009").asJava
  }

  "seven" should "have clinvarAnnotations with correct properties" in {
    val clinvar = seven.snpEffAnnotations.get.clinvarAnnotations.get

    clinvar.getCLNACC    shouldBe List("RCV000017599.1", "RCV000017600.1", "RCV000022558.1").asJava
    clinvar.getCLNDSDBID shouldBe List("C3150401", "CN043549", "C2751603:613060").asJava
  }

}
