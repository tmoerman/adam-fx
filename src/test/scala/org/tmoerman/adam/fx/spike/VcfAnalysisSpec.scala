package org.tmoerman.adam.fx.spike

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ShuffleRegionJoin
import org.tmoerman.adam.fx.BaseSparkContextSpec
import org.tmoerman.adam.fx.avro.AnnotatedGenotype

import org.tmoerman.adam.fx.snpeff.SnpEffContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import List.tabulate
import scala.reflect.ClassTag

import org.bdgenomics.adam.models.SequenceDictionary
import htsjdk.samtools.SAMSequenceRecord
import htsjdk.samtools.SAMSequenceDictionary
import Function._

/**
 * @author Thomas Moerman
 */
object VcfAnalysisSpec extends BaseSparkContextSpec {

  type RPKM = Double
  type GenotypeQuality = Double
  type Quality = Double
  type ReadDepth = Double

  type SweepValue = Double
  type SampleID = String
  type Contig = String
  type RefAllele = String
  type AltAllele = String
  type SampleGeneKey = String
  type Category = String
  type Start = Long
  type Count = Long
  type Coverage = Long

  type AnnotatedGenotypeWithRPKM = (AnnotatedGenotype, Option[RPKM])

  type UniqueVariantKeyNoAlleles = (SampleID, Contig, Start)
  type UniqueVariantKey = (SampleID, Contig, Start, RefAllele, AltAllele)

  type SweepValuePredicate = (SweepValue, (Option[Coverage], Option[AnnotatedGenotypeWithRPKM]) => Boolean)

  type DefaultValues = (ReadDepth, GenotypeQuality, Quality, RPKM)


  // 1. Load RPKM values per (sampleID, geneID) pair


  val wd = "/CCLE/50samples"

  //val rnaFile = wd + "/*RNA-08.vcf.exomefiltered.vcf.annotated.vcf"
  //val wxsFile = wd + "/*DNA-08.vcf.exomefiltered.vcf.annotated.vcf"
  val rnaFile = wd + "/F-36P-RNA-08.vcf.exomefiltered.vcf.annotated.vcf"
  val wxsFile = wd + "/F-36P-DNA-08.vcf.exomefiltered.vcf.annotated.vcf"
  val rpkmFile = wd + "/allSamples.rpkm"

  val sampleGeneKeyToRpkm: RDD[(SampleGeneKey, RPKM)] =
    sc.textFile(rpkmFile)
      .map(x => {
        val tmp = x.split("\t")
        (tmp(0), tmp(1).toDouble)
      })


  // 2. Functions related to aggregation keys


  def dropKey[T: ClassTag]: ((_, T)) => T = _._2

  def uniqueVariantKeyNoAlleles(annotatedGenotype: AnnotatedGenotype): UniqueVariantKeyNoAlleles = {
    val genotype = annotatedGenotype.getGenotype
    val variant = genotype.getVariant

    (genotype.getSampleId,
      variant.getContig.getContigName,
      variant.getStart)
  }

  def uniqueVariantKey(annotatedGenotype: AnnotatedGenotype): UniqueVariantKey = {
    val genotype = annotatedGenotype.getGenotype
    val variant = genotype.getVariant

    (genotype.getSampleId,
      variant.getContig.getContigName,
      variant.getStart,
      variant.getReferenceAllele,
      variant.getAlternateAllele)
  }

  def dropAlleles(uniqueVariantKey: UniqueVariantKey): UniqueVariantKeyNoAlleles =
    uniqueVariantKey match {
      case (sample, contig, start, _, _) => (sample, contig, start)
    }

  def keyedBySampleGeneKey(infix: String)(annotatedGenotype: AnnotatedGenotype): Seq[(SampleGeneKey, AnnotatedGenotype)] = {
    val sampleId = annotatedGenotype.getGenotype.getSampleId

    annotatedGenotype
      .getAnnotations
      .getFunctionalAnnotations
      .map(annotation => (sampleId + infix + annotation.getGeneID, annotatedGenotype))
  }

  def keyedBySampleGeneKeyRna = keyedBySampleGeneKey("_rna_") _

  def keyedBySampleGeneKeyWxs = keyedBySampleGeneKey("_wxs_") _


  // 3. Sequence dictionary


  val dict = wd + "/Homo_sapiens_assembly19.dict"

  val seqRecords = sc.textFile(dict).filter(x => x.startsWith("@SQ")).map(x => {
    val tmp = x.split("\t")
    new SAMSequenceRecord(tmp(1).split(":")(1), tmp(2).split(":")(1).toInt)
  }).collect().toList

  val samDict = new SAMSequenceDictionary(seqRecords)
  val seqDict = SequenceDictionary(samDict)


  def getCoverageMatchingSample(sample: String,
                                genotypes: RDD[AnnotatedGenotype],
                                bamFile: String): RDD[(UniqueVariantKeyNoAlleles, Coverage)] = {

    val bamRDD: RDD[ReferenceRegion] =
      sc.loadAlignments(bamFile)
        .filter(_.getReadMapped)
        .map(ReferenceRegion(_))

    val positionRDD = genotypes.map(g => {
      val variant = g.getGenotype.getVariant
      ReferenceRegion(variant.getContig.getContigName,
        variant.getStart,
        variant.getEnd)
    })

    val maxPartitions = bamRDD.partitions.length.toLong
    val partitionSize = seqDict.records.map(_.length).sum / maxPartitions

    val joinedRDD: RDD[(ReferenceRegion, ReferenceRegion)] =
      ShuffleRegionJoin(seqDict, partitionSize)
        .partitionAndJoin(positionRDD.keyBy(identity),
          bamRDD.keyBy(identity))

    joinedRDD
      .map { case (region, _) => (region, 1L) }
      .reduceByKey(_ + _)
      .map { case (region, record) => ((sample, region.referenceName, region.start), record) }
  }


  // 4. Load the annotated genotypes


  val wxsTmp: RDD[AnnotatedGenotype] = sc.loadAnnotatedGenotypes(wxsFile).cache()

  val rnaTmp: RDD[AnnotatedGenotype] = sc.loadAnnotatedGenotypes(rnaFile).cache()


  // 5. Calculate the Coverages


  val samplesFile = wd + "/samples.txt"

  //val sids = sc.textFile(samplesFile).collect
  val sampleIds = List("F-36P")

  val wxsCoverages: RDD[(UniqueVariantKeyNoAlleles, Coverage)] =
    sampleIds
      .map(sampleId => getCoverageMatchingSample(sampleId,
        rnaTmp.filter(x => x.getGenotype.getSampleId == sampleId),
        wd + "/ccle/" + sampleId.replace("/", "-") + "-DNA-08.bam"))
      .reduce(_ ++ _)


  val rnaCoverages: RDD[(UniqueVariantKeyNoAlleles, Coverage)] =
    sampleIds
      .map(sampleId => getCoverageMatchingSample(sampleId,
        wxsTmp.filter(x => x.getGenotype.getSampleId == sampleId),
        wd + "/ccle/" + sampleId.replace("/", "-") + "-RNA-08.bam"))
      .reduce(_ ++ _)


  val coveragesCogroup: RDD[(UniqueVariantKeyNoAlleles, (Option[Coverage], Option[Coverage]))] =
    wxsCoverages
      .cogroup(rnaCoverages)
      .map { case (uvkna, (it1, it2)) => (uvkna, (it1.headOption, it2.headOption)) }


  // 6. Join the annotated Genotypes with RPKM values


  def withMaxVariantCallErrorProbability(g1: AnnotatedGenotypeWithRPKM,
                                         g2: AnnotatedGenotypeWithRPKM): AnnotatedGenotypeWithRPKM = {

    def errorProbability(g: AnnotatedGenotypeWithRPKM) = g._1.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability

    if (errorProbability(g1) > errorProbability(g2)) g1 else g2
  }


  val wxsMap: RDD[(UniqueVariantKey, AnnotatedGenotypeWithRPKM)] =
    wxsTmp
      .flatMap(keyedBySampleGeneKeyWxs)
      .leftOuterJoin(sampleGeneKeyToRpkm)
      .map(dropKey)
      .map { case tuple@(annotatedGenotype, _) => (uniqueVariantKey(annotatedGenotype), tuple) }
      .reduceByKey(withMaxVariantCallErrorProbability)


  val rnaMap: RDD[(UniqueVariantKey, AnnotatedGenotypeWithRPKM)] =
    rnaTmp
      .flatMap(keyedBySampleGeneKeyRna)
      .leftOuterJoin(sampleGeneKeyToRpkm)
      .map(dropKey)
      .map { case tuple@(annotatedGenotype, _) => (uniqueVariantKey(annotatedGenotype), tuple) }
      .reduceByKey(withMaxVariantCallErrorProbability)


  // 7. Calculate sweep step factors


  val lnOf2 = math.log(2)

  val dpStep = math.floor(math.log(math.max(rnaTmp.map(g => g.getGenotype.getReadDepth).max,
    wxsTmp.map(g => g.getGenotype.getReadDepth).max)) / lnOf2)

  val qStep = math.floor(math.log(math.max(
    rnaTmp.map(g => g.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability).max,
    wxsTmp.map(g => g.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability).max)) / lnOf2)

  val rpkmStep = math.floor(math.log(sampleGeneKeyToRpkm.map(dropKey).max) / lnOf2)


  // 8. Calculate the coGroup


  val coGroup: RDD[(Option[Coverage], Option[AnnotatedGenotypeWithRPKM],
                    Option[Coverage], Option[AnnotatedGenotypeWithRPKM])] =
    wxsMap
      .cogroup(rnaMap)

      .map{ case (uniqueVariantKey, (wxsGenotypes, rnaGenotypes)) =>
        (dropAlleles(uniqueVariantKey), (wxsGenotypes.headOption, rnaGenotypes.headOption)) }
      .leftOuterJoin(coveragesCogroup)
      .map(dropKey)

      // merge the genotype and coverage data
      .map{ case ((wxsGenotypeOption, rnaGenotypeOption), Some((wxsCoverage, rnaCoverage))) =>
              (wxsCoverage, wxsGenotypeOption, rnaCoverage, rnaGenotypeOption)

            case ((wxsGenotypeOption, rnaGenotypeOption), None) =>
              (Some(0L), wxsGenotypeOption, Some(0L), rnaGenotypeOption)

            case _ => throw new Exception("wtf") }

      .cache()


  // 9. Category and Sweep Functions


  val CONCORDANCE = "WXS RNA concordance"
  val WXS_UNIQUE = "WXS unique"
  val WXS_DISCORDANT = "WXS discordant"
  val RNA_UNIQUE = "RNA unique"
  val RNA_DISCORDANT = "RNA discordant"
  val WTF = "WTF"

  val CATEGORIES = Seq(CONCORDANCE, WXS_UNIQUE, WXS_DISCORDANT, RNA_UNIQUE, RNA_DISCORDANT)

  def toCategory(minReadDepth: ReadDepth)
                (wxsCoverageOption: Option[Coverage],
                 wxsGenotypeOption: Option[AnnotatedGenotypeWithRPKM],
                 rnaCoverageOption: Option[Coverage],
                 rnaGenotypeOption: Option[AnnotatedGenotypeWithRPKM]): Category =

    (wxsCoverageOption, wxsGenotypeOption, rnaCoverageOption, rnaGenotypeOption) match {

      case (_, Some(wxsGT), _, Some(rnaGT))          => CONCORDANCE

      case (_, Some(wxsGT), None, None)              => WXS_UNIQUE
      case (_, Some(wxsGT), Some(rnaCoverage), None) => if (rnaCoverage >= minReadDepth) WXS_DISCORDANT else WXS_UNIQUE

      case (None, None, _, Some(rnaGT))              => RNA_UNIQUE
      case (Some(wxsCoverage), None, _, Some(rnaGT)) => if (wxsCoverage >= minReadDepth) RNA_DISCORDANT else RNA_UNIQUE

      case _ => WTF
    }


  def passesThresholds(readDepth: ReadDepth,
                       genotypeQuality: GenotypeQuality,
                       quality: Quality,
                       rpkmThreshold: RPKM)
                      (otherCoverageOption: Option[Coverage],
                       genotypeOption: Option[AnnotatedGenotypeWithRPKM]): Boolean = {

    lazy val genotypePassesSweepPredicates: Boolean =
      genotypeOption match {
        case Some((annotatedGenotype, rpkm)) =>
          val genotype = annotatedGenotype.getGenotype

          genotype.getReadDepth >= readDepth &&
            genotype.getGenotypeQuality >= genotypeQuality &&
            genotype.getVariantCallingAnnotations.getVariantCallErrorProbability >= quality &&
            rpkm.exists(_ >= rpkmThreshold)

        case _ => throw new Exception("should never happen")
      }

    val coverageSatisfiesReadDepthIfExists = otherCoverageOption.map(_ >= readDepth).getOrElse(true) // automatically passes if None

    coverageSatisfiesReadDepthIfExists && genotypePassesSweepPredicates
  }


  // 10. Generate count keys


  def toCountKeys(minimumReadDepth: ReadDepth,
                  sweepPredicates: Seq[SweepValuePredicate])
                 (wxsCoverageOption: Option[Coverage],
                  wxsGenotypeOption: Option[AnnotatedGenotypeWithRPKM],
                  rnaCoverageOption: Option[Coverage],
                  rnaGenotypeOption: Option[AnnotatedGenotypeWithRPKM]): Seq[(Category, SweepValue)] = {

    val category = toCategory(minimumReadDepth)(wxsCoverageOption, wxsGenotypeOption, rnaCoverageOption, rnaGenotypeOption)

    val passedPredicates = category match {

      case CONCORDANCE => sweepPredicates.takeWhile { case (v, predicate) => predicate(None, wxsGenotypeOption) &&
                                                                             predicate(None, rnaGenotypeOption) }

      case WXS_UNIQUE => sweepPredicates.takeWhile { case (v, predicate) => predicate(None, wxsGenotypeOption) }
      case WXS_DISCORDANT => sweepPredicates.takeWhile { case (v, predicate) => predicate(rnaCoverageOption, wxsGenotypeOption) }

      case RNA_UNIQUE => sweepPredicates.takeWhile { case (v, predicate) => predicate(None, rnaGenotypeOption) }
      case RNA_DISCORDANT => sweepPredicates.takeWhile { case (v, predicate) => predicate(wxsCoverageOption, rnaGenotypeOption) }

      case WTF => Nil
    }

    passedPredicates.map { case (v, _) => (category, v) }
  }


  // 11. Calculate sweep values and sweep predicates


  val nrSteps: Int = 50

  def powerStep(stepSize: Double)(step: Int) = math.pow(2, (step.toDouble / nrSteps) * stepSize)

  val readDepthSweepValues = tabulate(nrSteps)(powerStep(dpStep))

  val genotypeQualitySweepValues = tabulate(nrSteps)(_ * 100d / nrSteps)

  val qualitySweepValues = tabulate(nrSteps)(powerStep(qStep))

  val rpkmSweepValues = tabulate(nrSteps)(powerStep(rpkmStep))


  def toReadDepthSweepPredicates(defaultValues: DefaultValues): Seq[SweepValuePredicate] = defaultValues match {
    case (_, genotypeQuality, quality, rpkm) =>
      readDepthSweepValues.map(readDepth => (readDepth, passesThresholds(readDepth, genotypeQuality, quality, rpkm) _))
  }

  def toGenotypeQualitySweepPredicates(defaultValues: DefaultValues): Seq[SweepValuePredicate] = defaultValues match {
    case (readDepth, _, quality, rpkm) =>
      genotypeQualitySweepValues.map(genotypeQuality => (genotypeQuality, passesThresholds(readDepth, genotypeQuality, quality, rpkm) _))
  }

  def toQualitySweepPredicates(defaultValues: DefaultValues): Seq[SweepValuePredicate] = defaultValues match {
    case (readDepth, genotypeQuality, _, rpkm) =>
      qualitySweepValues.map(quality => (quality, passesThresholds(readDepth, genotypeQuality, quality, rpkm) _))
  }

  def toRpkmSweepPredicates(defaultValues: DefaultValues): Seq[SweepValuePredicate] = defaultValues match {
    case (readDepth, genotypeQuality, quality, _) =>
      rpkmSweepValues.map(rpkm => (rpkm, passesThresholds(readDepth, genotypeQuality, quality, rpkm) _))
  }


  // 12. Sweep counts


  def toZeroCounts(sweepValues: Seq[SweepValue]): Map[(Category, SweepValue), Count] =
    sweepValues.flatMap(x => CATEGORIES.map(category => ((category, x), 0L))).toMap

  def performSweepCounts(defaultValues: DefaultValues) = {
    val minimumReadDepth = defaultValues._1

    val readDepthCounts =
      toZeroCounts(readDepthSweepValues) ++
      coGroup
        .flatMap(tupled(toCountKeys(minimumReadDepth, toReadDepthSweepPredicates(defaultValues))))
        .countByValue()
        .toMap

    val qualityCounts =
      toZeroCounts(qualitySweepValues) ++
      coGroup
        .flatMap(tupled(toCountKeys(minimumReadDepth, toQualitySweepPredicates(defaultValues))))
        .countByValue()
        .toMap

    val genotypeQualityCounts =
      toZeroCounts(genotypeQualitySweepValues) ++
      coGroup
        .flatMap(tupled(toCountKeys(minimumReadDepth, toGenotypeQualitySweepPredicates(defaultValues))))
        .countByValue()
        .toMap

    val rpkmCounts =
      toZeroCounts(rpkmSweepValues) ++ // generate zero count placeholders
      coGroup                          // overwrite with actual values
        .flatMap(tupled(toCountKeys(minimumReadDepth, toRpkmSweepPredicates(defaultValues))))
        .countByValue()
        .toMap

    (readDepthCounts, genotypeQualityCounts, qualityCounts, rpkmCounts)
  }


  // 14. Chart generation


  class CountHolder(val category: Category, val threshold: SweepValue, val count: Count) extends Serializable

  def toCountHolder(x: ((Category, SweepValue), Count)) =
    x match { case ((cat, v), count) => new CountHolder(cat, v, count) }

  val defaultReadDepth:       ReadDepth       = 5
  val defaultGenotypeQuality: GenotypeQuality = 0
  val defaultQuality:         Quality         = 144
  val defaultRPKM:            RPKM            = 0



  // ---


}