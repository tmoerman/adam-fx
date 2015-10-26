package org.tmoerman.adam.fx.spike

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.rdd.{BroadcastRegionJoin, ShuffleRegionJoin}
import org.bdgenomics.formats.avro.{Variant, Contig, Feature}
import org.tmoerman.adam.fx.BaseSparkContextSpec
import org.tmoerman.adam.fx.avro.AnnotatedGenotype

import org.tmoerman.adam.fx.snpeff.SnpEffContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import List.tabulate
import scala.reflect.ClassTag
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
  type ROCLabel = String

  type AnnotatedGenotypeWithRPKM = (AnnotatedGenotype, Option[RPKM])

  type UniqueVariantKeyNoAlleles = (SampleID, Contig, Start)
  type UniqueVariantKey = (SampleID, Contig, Start, RefAllele, AltAllele)

  type ThresholdPredicate = (Option[Coverage], Option[AnnotatedGenotypeWithRPKM]) => Boolean

  type DefaultValues = (ReadDepth, GenotypeQuality, Quality, RPKM)

  type WxsRnaCoGroupRow = (Option[Coverage], Option[AnnotatedGenotypeWithRPKM],
                           Option[Coverage], Option[AnnotatedGenotypeWithRPKM])

  type RowFilter = (RDD[WxsRnaCoGroupRow]) => (RDD[WxsRnaCoGroupRow])


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


  def toRegion(g: AnnotatedGenotype): ReferenceRegion = {
    val variant: Variant = g.getGenotype.getVariant

    ReferenceRegion(variant.getContig.getContigName,
                    variant.getStart,
                    variant.getEnd)
  }

  def getCoverageMatchingSample(sample: String,
                                genotypes: RDD[AnnotatedGenotype],
                                bamFile: String): RDD[(UniqueVariantKeyNoAlleles, Coverage)] = {

    val bamRDD: RDD[ReferenceRegion] =
      sc.loadAlignments(bamFile)
        .filter(_.getReadMapped)
        .map(ReferenceRegion(_))

    val positionRDD = genotypes.map(toRegion)

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
      .keyBy{ case (annotatedGenotype, _) => uniqueVariantKey(annotatedGenotype) }
      .reduceByKey(withMaxVariantCallErrorProbability)


  val rnaMap: RDD[(UniqueVariantKey, AnnotatedGenotypeWithRPKM)] =
    rnaTmp
      .flatMap(keyedBySampleGeneKeyRna)
      .leftOuterJoin(sampleGeneKeyToRpkm)
      .map(dropKey)
      .keyBy{ case (annotatedGenotype, _) => uniqueVariantKey(annotatedGenotype) }
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


  val coGroup: RDD[WxsRnaCoGroupRow] =
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


  def toCategory(minReadDepth: ReadDepth)(wxsRnaCoGroupRow: WxsRnaCoGroupRow): Category =
    wxsRnaCoGroupRow match {

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

  val concordanceThresholdPredicateApplication = (predicate: ThresholdPredicate) => (wxsRnaCoGroupRow: WxsRnaCoGroupRow) => wxsRnaCoGroupRow match {
    case (_, wxsGenotypeOption, _, rnaGenotypeOption) => predicate(None, wxsGenotypeOption) &&
                                                         predicate(None, rnaGenotypeOption)
  }

  val wxsUniqueThresholdPredicateApplication = (predicate: ThresholdPredicate) => (wxsRnaCoGroupRow: WxsRnaCoGroupRow) => wxsRnaCoGroupRow match {
    case (_, wxsGenotypeOption, _, _) => predicate(None, wxsGenotypeOption)
  }

  val wxsDiscordantThresholdPredicateApplication = (predicate: ThresholdPredicate) => (wxsRnaCoGroupRow: WxsRnaCoGroupRow) => wxsRnaCoGroupRow match {
    case (_, wxsGenotypeOption, rnaCoverageOption, _) => predicate(rnaCoverageOption, wxsGenotypeOption)
  }

  val rnaUniqueThresholdPredicateApplication = (predicate: ThresholdPredicate) => (wxsRnaCoGroupRow: WxsRnaCoGroupRow) => wxsRnaCoGroupRow match {
    case (_, _, _, rnaGenotypeOption) => predicate(None, rnaGenotypeOption)
  }

  val rnaDiscordantThresholdPredicateApplication = (predicate: ThresholdPredicate) => (wxsRnaCoGroupRow: WxsRnaCoGroupRow) => wxsRnaCoGroupRow match {
    case (wxsCoverageOption, _, _, rnaGenotypeOption) => predicate(wxsCoverageOption, rnaGenotypeOption)
  }

  val thresholdPredicateApplicationBy: Map[Category, (ThresholdPredicate) => (WxsRnaCoGroupRow) => Boolean] =
    Map(CONCORDANCE    -> concordanceThresholdPredicateApplication,
        WXS_UNIQUE     -> wxsUniqueThresholdPredicateApplication,
        WXS_DISCORDANT -> wxsDiscordantThresholdPredicateApplication,
        RNA_UNIQUE     -> rnaUniqueThresholdPredicateApplication,
        RNA_DISCORDANT -> rnaDiscordantThresholdPredicateApplication)


  def toCountKeys(sweepPredicates: Seq[(SweepValue, ThresholdPredicate)])
                 (category: Category, wxsRnaCoGroupRow: WxsRnaCoGroupRow): Seq[(Category, SweepValue)] =
    sweepPredicates
      .takeWhile { case (_, predicate) => thresholdPredicateApplicationBy(category)(predicate)(wxsRnaCoGroupRow) }
      .map { case (sweepValue, _) => (category, sweepValue) }


  // 11. Calculate sweep values and sweep predicates


  val nrSteps: Int = 50

  def powerStep(stepSize: Double)(step: Int) = math.pow(2, (step.toDouble / nrSteps) * stepSize)

  val readDepthSweepValues = tabulate(nrSteps)(powerStep(dpStep))

  val genotypeQualitySweepValues = tabulate(nrSteps)(_ * 100d / nrSteps)

  val qualitySweepValues = tabulate(nrSteps)(powerStep(qStep))

  val rpkmSweepValues = tabulate(nrSteps)(powerStep(rpkmStep))


  def toReadDepthSweepPredicates(defaultValues: DefaultValues): Seq[(SweepValue, ThresholdPredicate)] = defaultValues match {
    case (_, genotypeQuality, quality, rpkm) =>
      readDepthSweepValues.map(readDepth => (readDepth, passesThresholds(readDepth, genotypeQuality, quality, rpkm) _))
  }

  def toGenotypeQualitySweepPredicates(defaultValues: DefaultValues): Seq[(SweepValue, ThresholdPredicate)] = defaultValues match {
    case (readDepth, _, quality, rpkm) =>
      genotypeQualitySweepValues.map(genotypeQuality => (genotypeQuality, passesThresholds(readDepth, genotypeQuality, quality, rpkm) _))
  }

  def toQualitySweepPredicates(defaultValues: DefaultValues): Seq[(SweepValue, ThresholdPredicate)] = defaultValues match {
    case (readDepth, genotypeQuality, _, rpkm) =>
      qualitySweepValues.map(quality => (quality, passesThresholds(readDepth, genotypeQuality, quality, rpkm) _))
  }

  def toRpkmSweepPredicates(defaultValues: DefaultValues): Seq[(SweepValue, ThresholdPredicate)] = defaultValues match {
    case (readDepth, genotypeQuality, quality, _) =>
      rpkmSweepValues.map(rpkm => (rpkm, passesThresholds(readDepth, genotypeQuality, quality, rpkm) _))
  }

  def toBaselinePredicate(defaultValues: DefaultValues): ThresholdPredicate = defaultValues match {
    case (readDepth, genotypeQuality, quality, rpkm) =>
      val result = passesThresholds(readDepth, genotypeQuality, quality, rpkm) _ // makes stupid warning disappear
      result
  }

  // 12. Sweep counts


  def toZeroCounts(sweepValues: Seq[SweepValue]): Map[(Category, SweepValue), Count] =
    sweepValues.flatMap(x => CATEGORIES.map(category => ((category, x), 0L))).toMap


  def toROC(baselineCounts: Map[Category, Count],
            filteredCounts: Map[Category, Count]) = {

    val truePos = filteredCounts(CONCORDANCE)
    val condPos = baselineCounts(CONCORDANCE)

    val falsePosWxs = filteredCounts(WXS_DISCORDANT)
    val condNegWxs  = baselineCounts(WXS_DISCORDANT)

    val falsePosRna = filteredCounts(RNA_DISCORDANT)
    val condNegRna  = baselineCounts(RNA_DISCORDANT)

    val tpr    = truePos.toDouble / condPos
    val fprWxs = falsePosWxs.toDouble / condNegWxs
    val fprRna = falsePosRna.toDouble / condNegRna

    Seq(("WXS", tpr, fprWxs), ("RNA", tpr, fprRna))
  }


  def performSweepCounts(
      defaultValues: DefaultValues,
      filter: (RDD[WxsRnaCoGroupRow]) => (RDD[WxsRnaCoGroupRow]) = identity) = {

    val filtered = filter(coGroup)

    val minimumReadDepth = defaultValues._1

    val coGroupByCategory = filtered.keyBy(toCategory(minimumReadDepth)).cache()

    val baselineCounts: Map[Category, Count] =
      coGroupByCategory
        .filter{ case (category, row) => thresholdPredicateApplicationBy(category)(toBaselinePredicate(defaultValues))(row) }
        .countByKey()
        .toMap

    val readDepthCounts: Map[(Category, SweepValue), Count] =
      toZeroCounts(readDepthSweepValues) ++
      coGroupByCategory
        .flatMap(tupled(toCountKeys(toReadDepthSweepPredicates(defaultValues))))
        .countByValue()
        .toMap

    val readDepthROCdata =
      readDepthCounts
        .toList
        .groupBy{ case ((_, sweepValue), _) => sweepValue }
        .mapValues(_.foldLeft(Map[Category, Count]()) { case (m, ((cat, _), count)) => m + (cat -> count) })
        .flatMap{ case (v, filteredCounts) => toROC(baselineCounts, filteredCounts).map{ case (label, tpr, fpr) => (v, label, tpr, fpr) } }

    val qualityCounts: Map[(Category, SweepValue), Count] =
      toZeroCounts(qualitySweepValues) ++
      coGroupByCategory
        .flatMap(tupled(toCountKeys(toQualitySweepPredicates(defaultValues))))
        .countByValue()
        .toMap

    val genotypeQualityCounts: Map[(Category, SweepValue), Count] =
      toZeroCounts(genotypeQualitySweepValues) ++
      coGroupByCategory

        .flatMap(tupled(toCountKeys(toGenotypeQualitySweepPredicates(defaultValues))))
        .countByValue()
        .toMap

    val rpkmCounts: Map[(Category, SweepValue), Count] =
      toZeroCounts(rpkmSweepValues) ++ // generate zero count placeholders
      coGroupByCategory                // overwrite with actual values
        .flatMap(tupled(toCountKeys(toRpkmSweepPredicates(defaultValues))))
        .countByValue()
        .toMap

    (readDepthCounts, genotypeQualityCounts, qualityCounts, rpkmCounts)
  }


  def matchesFeaturesShuffle(features: RDD[Feature])(rows: RDD[WxsRnaCoGroupRow]) = {

    val rowsByReferenceRegion: RDD[(ReferenceRegion, WxsRnaCoGroupRow)] =
      rows
        .keyBy(referenceRegion)
        .cache()

//    val keysByReferenceRegion: RDD[(ReferenceRegion, UniqueVariantKey)] =
//      rowsByRegion
//        .mapValues(row => firstGenotype(row).map(uniqueVariantKey).get)

    val maxPartitions = rows.partitions.length.toLong
    val partitionSize = seqDict.records.map(_.length).sum / maxPartitions

    val matchedRows: RDD[WxsRnaCoGroupRow] =
      new ShuffleRegionJoin(seqDict, partitionSize)
        .partitionAndJoin(rowsByReferenceRegion, features.keyBy(referenceRegion))
        .map(_._1)

    matchedRows
  }

  def matchesFeaturesBroadcast(features: RDD[Feature])(rows: RDD[WxsRnaCoGroupRow]): RDD[WxsRnaCoGroupRow] = {
    BroadcastRegionJoin
      .partitionAndJoin(rows.keyBy(referenceRegion),
                        features.keyBy(referenceRegion))
      .map(_._1)
  }

  def referenceRegion(feat: Feature): ReferenceRegion = ReferenceRegion(feat)

  def firstGenotype(row: WxsRnaCoGroupRow): Option[AnnotatedGenotype] = List(row._2, row._4).find(_.isDefined).map(_.get._1)

  def referenceRegion(row: WxsRnaCoGroupRow): ReferenceRegion = firstGenotype(row).map(toRegion).get


  def parseBED(parseOtherFields: Boolean)(line: String): Seq[Feature] = {

    import java.util.{Map => JMap}

    if (line.startsWith("#")) {
      return Seq()
    }

    val fields = line.split("\t")

    val cb = Contig.newBuilder()
    cb.setContigName(fields(0))

    val fb = Feature.newBuilder()
    fb.setContig(cb.build())
    fb.setFeatureId(UUID.randomUUID().toString)

    // BED files are 0-based space-coordinates, so conversion to
    // our coordinate space should mean that the values are unchanged.
    fb.setStart(fields(1).toLong)
    fb.setEnd(fields(2).toLong)

    if (parseOtherFields) {
      val attributes: Map[String, String] =
        (4 to fields.length)
          .map(_.toString)
          .zip(fields.drop(3))
          .toMap

      fb.setAttributes(attributes)
    }

    Seq(fb.build())
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