package org.tmoerman.adam.fx.filter

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, ReferencePosition}
import org.bdgenomics.formats.avro.Feature
import org.tmoerman.adam.fx.BaseSparkContextSpec
import org.bdgenomics.adam.rdd.ADAMContext._
import org.tmoerman.adam.fx.avro.AnnotatedGenotype
import org.tmoerman.adam.fx.snpeff.SnpEffContext._
import org.bdgenomics.adam.rdd.BroadcastRegionJoin

/**
 * @author Thomas Moerman
 */
class FilterByRegionSpikeSpec extends BaseSparkContextSpec {

  val truX = "/Users/tmo/Work/exascience/cellpass/bed/TruSeq_Exome_b37.bed"

  val vcfDir = "/Users/tmo/Work/exascience/cellpass/ddecap/50samples/"

  val dna = vcfDir + "ALL-SIL-DNA-08.annotated.vcf"

  val rna = vcfDir + "ALL-SIL-RNA-08.annotated.vcf"

  "testing" should "work" in {

    val queryGene = "BRCA"

    def toPosition(g: AnnotatedGenotype): ReferenceRegion = ReferencePosition(g.getGenotype.getVariant)

    def toRegion(g: AnnotatedGenotype): ReferenceRegion = {
      val variant = g.getGenotype.getVariant

      ReferenceRegion(variant.getContig.getContigName,
                      variant.getStart,
                      variant.getEnd)
    }

    def fetchAnnotationsForFeature(rdd: RDD[(Feature, AnnotatedGenotype)]): String =
      rdd
        .collect()
        .map{case (feature, annotatedGenotype) => { val name = feature.getFeatureType

                                                    val geneAnnotations = annotatedGenotype
                                                                            .getAnnotations
                                                                            .getFunctionalAnnotations
                                                                            .map(fa => fa.getGeneName)
                                                                            .mkString(", ")

                                                    s"BED feature: ($name) -> SnpEff annotations: ($geneAnnotations)"}}
        .mkString("\n")

    val truXRDD: RDD[Feature] = sc.loadBED(truX)

    val dnaGenotypesRDD = sc.loadAnnotatedGenotypes(dna)

    val rnaGenotypesRDD = sc.loadAnnotatedGenotypes(rna)

    val brcaRDD = truXRDD.filter(feat => feat.getFeatureType.startsWith(queryGene))

    val brcaRegions = brcaRDD.keyBy(ReferenceRegion(_)).cache()

    val dnaTuples = dnaGenotypesRDD.keyBy(toRegion)

    val rnaTuples = rnaGenotypesRDD.keyBy(toRegion)

    val dnaBrca = BroadcastRegionJoin.partitionAndJoin(brcaRegions, dnaTuples).cache()

    val rnaBrca = BroadcastRegionJoin.partitionAndJoin(brcaRegions, rnaTuples).cache()

    println(s"$dna:")
    println(fetchAnnotationsForFeature(dnaBrca))

    println(s"$rna:")
    println(fetchAnnotationsForFeature(rnaBrca))
  }

}
