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

  type RPKM          = Double
  type SweepValue    = Double
  type SampleID      = String
  type Contig        = String
  type RefAllele     = String
  type AltAllele     = String
  type SampleGeneKey = String
  type Category      = String
  type Start         = Long
  type Count         = Long
  type Coverage      = Long

  type AnnotatedGenotypeWithRPKM = (AnnotatedGenotype, Option[RPKM])

  type UniqueVariantKeyNoAlleles = (SampleID, Contig, Start)
  type UniqueVariantKey          = (SampleID, Contig, Start, RefAllele, AltAllele)

  type SweepValuePredicate = (SweepValue, (Option[Coverage], Option[AnnotatedGenotypeWithRPKM]) => Boolean)

  // ---


  val wd = "/CCLE/50samples"

  //val rnaFile = wd + "/*RNA-08.vcf.exomefiltered.vcf.annotated.vcf"
  //val wxsFile = wd + "/*DNA-08.vcf.exomefiltered.vcf.annotated.vcf"
  val rnaFile  = wd + "/F-36P-RNA-08.vcf.exomefiltered.vcf.annotated.vcf"
  val wxsFile  = wd + "/F-36P-DNA-08.vcf.exomefiltered.vcf.annotated.vcf"
  val rpkmFile = wd + "/allSamples.rpkm"

  val sampleGeneKeyToRpkm: RDD[(SampleGeneKey, RPKM)] =
    sc.textFile(rpkmFile)
      .map(x => {
        val tmp = x.split("\t")
        (tmp(0), tmp(1).toDouble)
      })
  

  // ---


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



  // ---


  val dict = wd + "/Homo_sapiens_assembly19.dict"

  val seqRecords = sc.textFile(dict).filter(x => x.startsWith("@SQ")).map(x => {
    val tmp = x.split("\t")
    new SAMSequenceRecord(tmp(1).split(":")(1), tmp(2).split(":")(1).toInt)
  }).collect().toList

  val samDict = new SAMSequenceDictionary(seqRecords)
  val seqDict = SequenceDictionary(samDict)


  // ---


  def getCoverageMatchingSample(
    sample: String,
    genotypes: RDD[AnnotatedGenotype],
    bamFile: String): RDD[(UniqueVariantKeyNoAlleles, Coverage)] = {

    val bamRDD: RDD[ReferenceRegion] =
      sc.loadAlignments(bamFile)
        .filter(_.getReadMapped)
        .map(ReferenceRegion(_))

    val positionRDD = genotypes.map(g => {val variant = g.getGenotype.getVariant
                                          ReferenceRegion(variant.getContig.getContigName,
                                                          variant.getStart,
                                                          variant.getEnd)})

    val maxPartitions = bamRDD.partitions.length.toLong
    val partitionSize = seqDict.records.map(_.length).sum / maxPartitions

    val joinedRDD: RDD[(ReferenceRegion, ReferenceRegion)] =
      ShuffleRegionJoin(seqDict, partitionSize)
        .partitionAndJoin(positionRDD.keyBy(identity),
                          bamRDD.keyBy(identity))

    joinedRDD
      .map { case (region, _) => (region, 1L) }
      .reduceByKey(_ + _)
      .map{case (region, record) => ((sample, region.referenceName, region.start), record)}
  }


  // ---


  val wxsTmp: RDD[AnnotatedGenotype] = sc.loadAnnotatedGenotypes(wxsFile).cache()

  val rnaTmp: RDD[AnnotatedGenotype] = sc.loadAnnotatedGenotypes(rnaFile).cache()


  // 5. Calculate the Coverages


  val samplesFile =  wd + "/samples.txt"

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
      .map{ case (uvkna, (it1, it2)) => (uvkna, (it1.headOption, it2.headOption))}


  // 6. Join the annotated Genotypes with RPKM values


  def withMaxVariantCallErrorProbability(
      g1: AnnotatedGenotypeWithRPKM,
      g2: AnnotatedGenotypeWithRPKM): AnnotatedGenotypeWithRPKM = {

    def errorProbability(g: AnnotatedGenotypeWithRPKM) = g._1.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability

    if (errorProbability(g1) > errorProbability(g2)) g1 else g2
  }


  val wxsMap: RDD[(UniqueVariantKey, AnnotatedGenotypeWithRPKM)] =
    wxsTmp
      .flatMap(keyedBySampleGeneKeyWxs)
      .leftOuterJoin(sampleGeneKeyToRpkm)
      .map(dropKey)
      .map{ case tuple@(annotatedGenotype, _) => (uniqueVariantKey(annotatedGenotype), tuple) }
      .reduceByKey(withMaxVariantCallErrorProbability)


  val rnaMap: RDD[(UniqueVariantKey, AnnotatedGenotypeWithRPKM)] =
    rnaTmp
      .flatMap(keyedBySampleGeneKeyRna)
      .leftOuterJoin(sampleGeneKeyToRpkm)
      .map(dropKey)
      .map{ case tuple@(annotatedGenotype, _) => (uniqueVariantKey(annotatedGenotype), tuple) }
      .reduceByKey(withMaxVariantCallErrorProbability)


  // 7. Calculate sweep step factors


  val lnOf2 = math.log(2)

  val dpStep = math.floor(math.log(math.max(rnaTmp.map(g => g.getGenotype.getReadDepth).max,
    wxsTmp.map(g => g.getGenotype.getReadDepth).max)) / lnOf2)

  val qStep = math.floor(math.log(math.max(
    rnaTmp.map(g => g.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability).max,
    wxsTmp.map(g => g.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability).max)) / lnOf2)

  val rpkmStep =  math.floor(math.log(sampleGeneKeyToRpkm.map(dropKey).max) / lnOf2)


  // 8. Default filter values


  val defaultReadDepth = 10
  val defaultGenotypeQuality = 50
  val defaultQuality = 250
  val defaultRPKM = 0

  val nrSteps = 50


  // 9. Calculate the coGroup


  val coGroupRaw =
    wxsMap
      .cogroup(rnaMap)
      .map{ case (uniqueVariantKey, (wxsGenotypes, rnaGenotypes)) =>
        (dropAlleles(uniqueVariantKey), (wxsGenotypes.headOption, rnaGenotypes.headOption)) }
      .leftOuterJoin(coveragesCogroup)
      .map(dropKey)
      .cache()

  // statistics
  val coGroupEmptyCoverages =
    coGroupRaw
      .filter {case (_, coverages) => coverages.isEmpty}
      .count()

  // statistics
  val coGroupWithCoverages =
    coGroupRaw
      .filter {case (_, coverages) => coverages.isDefined}
      .count()

  val coGroup: RDD[(Option[Coverage], Option[AnnotatedGenotypeWithRPKM],
                    Option[Coverage], Option[AnnotatedGenotypeWithRPKM])] =
    coGroupRaw

      .filter { case (_, coveragesOption) => coveragesOption.isDefined }

      // merge the genotype and coverage data
      .map{ case ((wxsGenotypeOption, rnaGenotypeOption), Some((wxsCoverage, rnaCoverage))) =>
              (wxsCoverage, wxsGenotypeOption, rnaCoverage, rnaGenotypeOption)

            // this case is WEIRD -> TODO investigate
//            case ((wxsGenotypeOption, rnaGenotypeOption), None) =>
//              (None, wxsGenotypeOption, None, rnaGenotypeOption)

            case _ => throw new IllegalArgumentException("wtf") }

      .cache()


  val CONCORDANCE    = "WXS RNA concordance"
  val WXS_UNIQUE     = "WXS unique"
  val WXS_DISCORDANT = "WXS discordant"
  val RNA_UNIQUE     = "RNA unique"
  val RNA_DISCORDANT = "RNA discordant"
  val WTF            = "WTF"

  val CATEGORIES = Seq(CONCORDANCE, WXS_UNIQUE, WXS_DISCORDANT, RNA_UNIQUE, RNA_DISCORDANT)

  def toCategory(minReadDepth: Coverage)
                (wxsCoverageOption: Option[Coverage],
                 wxsGenotypeOption: Option[AnnotatedGenotypeWithRPKM],
                 rnaCoverageOption: Option[Coverage],
                 rnaGenotypeOption: Option[AnnotatedGenotypeWithRPKM]): Category =

    (wxsCoverageOption, wxsGenotypeOption, rnaCoverageOption, rnaGenotypeOption) match {

      case (_                , Some(wxsGT), _                , Some(rnaGT)) => CONCORDANCE

      case (_                , Some(wxsGT), None             , None       ) => WXS_UNIQUE
      case (_                , Some(wxsGT), Some(rnaCoverage), None       ) => if (rnaCoverage >= minReadDepth)
                                                                                 WXS_DISCORDANT else WXS_UNIQUE

      case (None             , None       , _                , Some(rnaGT)) => RNA_UNIQUE
      case (Some(wxsCoverage), None       , _                , Some(rnaGT)) => if (wxsCoverage >= minReadDepth)
                                                                                 RNA_DISCORDANT else RNA_UNIQUE

      case _ => WTF
    }


  def passesThresholds(depth:           Double,
                       genotypeQuality: Double,
                       quality:         Double,
                       rpkmThreshold:   Double)
                      (otherCoverageOption: Option[Coverage],
                       genotypeOption:      Option[AnnotatedGenotypeWithRPKM]): Boolean = {

    lazy val genotypePassesSweepPredicates: Boolean =
      genotypeOption match {
        case Some((annotatedGenotype, rpkm)) =>
          val genotype = annotatedGenotype.getGenotype

          genotype.getReadDepth                                                >= depth           &&
          genotype.getGenotypeQuality                                          >= genotypeQuality &&
          genotype.getVariantCallingAnnotations.getVariantCallErrorProbability >= quality         &&
          rpkm.exists(_ >= rpkmThreshold)
        case _ => throw new IllegalArgumentException("should never happen")
      }

    val coverageSatisfiesReadDepthIfExists = otherCoverageOption.map(_ >= depth).getOrElse(true) // automatically passes if None

    coverageSatisfiesReadDepthIfExists && genotypePassesSweepPredicates
  }

  def toCategorySweepValues(sweepPredicates: Seq[SweepValuePredicate])
                           (wxsCoverageOption: Option[Coverage],
                            wxsGenotypeOption: Option[AnnotatedGenotypeWithRPKM],
                            rnaCoverageOption: Option[Coverage],
                            rnaGenotypeOption: Option[AnnotatedGenotypeWithRPKM]): Seq[(Category, SweepValue)] = {

    val category = toCategory(defaultReadDepth) (wxsCoverageOption, wxsGenotypeOption, rnaCoverageOption, rnaGenotypeOption)

    val passedPredicates = category match {

      case CONCORDANCE    => sweepPredicates.takeWhile { case (v, predicate) => predicate(None, wxsGenotypeOption) &&
                                                                                predicate(None, rnaGenotypeOption) }

      case WXS_UNIQUE     => sweepPredicates.takeWhile { case (v, predicate) => predicate(None, wxsGenotypeOption) }
      case WXS_DISCORDANT => sweepPredicates.takeWhile { case (v, predicate) => predicate(rnaCoverageOption, wxsGenotypeOption) }

      case RNA_UNIQUE     => sweepPredicates.takeWhile { case (v, predicate) => predicate(None, rnaGenotypeOption) }
      case RNA_DISCORDANT => sweepPredicates.takeWhile { case (v, predicate) => predicate(wxsCoverageOption, rnaGenotypeOption) }

      case WTF => Nil
    }
    
    passedPredicates.map { case (v, _) => (category, v) }
  }


  // ---

  
  def powerStep(stepSize: Double)(step: Int) = math.pow(2, (step.toDouble / nrSteps) * stepSize)

  val rpkmSweepValues            = tabulate(nrSteps)(powerStep(rpkmStep))
  
  val qualitySweepValues         = tabulate(nrSteps)(powerStep(qStep))

  val readDepthSweepValues       = tabulate(nrSteps)(powerStep(dpStep))

  val genotypeQualitySweepValues = tabulate(nrSteps)(_ * 100d / nrSteps)


  val rpkmSweepPredicates: Seq[SweepValuePredicate] =
    rpkmSweepValues.map(rpkm => (rpkm, passesThresholds(defaultReadDepth, defaultGenotypeQuality, defaultQuality, rpkm) _))

  val qualitySweepPredicates: Seq[SweepValuePredicate] =
    qualitySweepValues.map(quality => (quality, passesThresholds(defaultReadDepth, defaultGenotypeQuality, quality, defaultRPKM) _))

  val readDepthSweepPredicates: Seq[SweepValuePredicate] =
    readDepthSweepValues.map(readDepth => (readDepth, passesThresholds(readDepth, defaultGenotypeQuality, defaultQuality, defaultRPKM) _))

  val genotypeQualitySweepPredicates: Seq[SweepValuePredicate] =
    genotypeQualitySweepValues.map(gtQuality => (gtQuality, passesThresholds(defaultReadDepth, gtQuality, defaultQuality, defaultRPKM) _))


  // ---


  def toZeroCounts(sweepValues: Seq[SweepValue]): Map[(Category, SweepValue), Count] = {
    def f2(x: SweepValue) = CATEGORIES.map(category => ((category, x), 0L))
    
    sweepValues.flatMap(f2).toMap
  }

  val rpkmRange: Map[(Category, SweepValue), Count] =
    toZeroCounts(rpkmSweepValues) ++ // generate zero count placeholders
    coGroup                          // overwrite with actual values
      .flatMap(tupled(toCategorySweepValues(rpkmSweepPredicates)))
      .countByValue()
      .toMap


  val qRange =
    toZeroCounts(qualitySweepValues) ++
    coGroup
      .flatMap(tupled(toCategorySweepValues(qualitySweepPredicates)))
      .countByValue()
      .toMap


  val dpRange =
    toZeroCounts(readDepthSweepValues) ++
    coGroup
      .flatMap(tupled(toCategorySweepValues(readDepthSweepPredicates)))
      .countByValue()
      .toMap


  val gqRange =
    toZeroCounts(genotypeQualitySweepValues) ++
    coGroup
      .flatMap(tupled(toCategorySweepValues(genotypeQualitySweepPredicates)))
      .countByValue()
      .toMap


  // ---


  case class CountRepr(category: Category, threshold: SweepValue, count: Count) extends Serializable

  def toCountRepr(x: ((Category, SweepValue), Count)) =
    x match { case ((cat, v), count) => CountRepr(cat, v, count) }



  // --




//  val rpkmRange: List[CountRepr] = tabulate(nrSteps){ step => {
//
//      val minRpkm = math.pow(2, (step.toDouble / nrSteps) * rpkmStep)
//
//      val passesWxsThresholds = passesThresholds(wxsDepth, wxsGenotypeQuality, wxsQuality, minRpkm)(_)
//      val passesRnaThresholds = passesThresholds(rnaDepth, rnaGenotypeQuality, rnaQuality, minRpkm)(_)
//
//      List(
//
//        (minRpkm, "concordance",
//          both.filter{ case (wxsGenotype, rnaGenotype) => passesWxsThresholds(wxsGenotype) &&
//                                                          passesRnaThresholds(rnaGenotype) }
//            .count()),
//
//        (minRpkm, "rnaOnly",
//          rna.filter{ case tuple@(_, _, otherCoverage) => otherCoverage.getOrElse(0l) >= wxsDepth &&
//                                                          passesRnaThresholds(tuple) }
//            .count()),
//
//        (minRpkm, "wxsOnly",
//          wxs.filter{ case tuple@(_, _, otherCoverage) => otherCoverage.getOrElse(0l) >= rnaDepth &&
//                                                          passesWxsThresholds(tuple) }
//            .count()))}}
//
//      .flatten


  // ---


//  val qRange: List[CountRepr] = List.tabulate(nrSteps){ step => {
//
//    val minQuality = math.pow(2, (step.toDouble / nrSteps) * Qstep)
//
//    val passesWxsThresholds = passesThresholds(wxsDepth, wxsGenotypeQuality, minQuality, wxsRpkm)(_)
//    val passesRnaThresholds = passesThresholds(rnaDepth, rnaGenotypeQuality, minQuality, rnaRpkm)(_)
//
//    List(
//
//      (minQuality, "concordance",
//        both.filter{ case (wxsGenotype, rnaGenotype) => passesWxsThresholds(wxsGenotype) &&
//                                                        passesRnaThresholds(rnaGenotype) }
//            .count()),
//
//      (minQuality, "rnaOnly",
//        rna.filter{ case tuple@(_, _, otherCoverage) => otherCoverage.getOrElse(0l) >= wxsDepth &&
//                                                        passesRnaThresholds(tuple) }
//           .count()),
//
//      (minQuality, "wxsOnly",
//        wxs.filter{ case tuple@(_, _, otherCoverage) => otherCoverage.getOrElse(0l) >= rnaDepth &&
//                                                        passesWxsThresholds(tuple) }
//           .count()))}}
//
//    .flatten


  // ---


//  val dpRange = tabulate(nrSteps){ step => {
//
//    val minReadDepth = math.pow(2, (step.toDouble / nrSteps) * DPstep)
//
//    val passesWxsThresholds = passesThresholds(minReadDepth, wxsGenotypeQuality, wxsQuality, wxsRpkm)(_)
//    val passesRnaThresholds = passesThresholds(minReadDepth, rnaGenotypeQuality, rnaQuality, rnaRpkm)(_)
//
//    List(
//
//      (minReadDepth, "concordance",
//        both.filter{ case (wxsGenotype, rnaGenotype) => passesWxsThresholds(wxsGenotype) &&
//                                                        passesRnaThresholds(rnaGenotype) }
//            .count()),
//
//      (minReadDepth, "rnaOnly",
//        rna.filter{ case tuple@(_, _, otherCoverage) => otherCoverage.getOrElse(0l) >= minReadDepth &&
//                                                        passesRnaThresholds(tuple) }
//           .count()),
//
//      (minReadDepth, "wxsOnly",
//        wxs.filter{ case tuple@(_, _, otherCoverage) => otherCoverage.getOrElse(0l) >= minReadDepth &&
//                                                        passesWxsThresholds(tuple) }
//           .count()))}}
//
//    .flatten



  // ---

  object Bla {

    //    val passesWxsThresholds = passesThresholds(wxsDepth, wxsGenotypeQuality, wxsQuality, wxsRpkm)(_)
    //
    //    val passesRnaThresholds = passesThresholds(rnaDepth, rnaGenotypeQuality, rnaQuality, wxsRpkm)(_)
    //
    //    val tp =
    //      both.filter{ case (wxsGenotype, rnaGenotype) => passesWxsThresholds(wxsGenotype) &&
    //        passesRnaThresholds(rnaGenotype) }
    //        .count()
    //
    //    val rnaCount = rna.count()
    //
    //    val filteredRnaCount =
    //      rna.filter{ case tuple@(_, _, otherCoverage) => otherCoverage.getOrElse(0l) >= wxsDepth &&
    //                                                      passesRnaThresholds(tuple) }
    //        .count()
    //
    //    val wxsCount = wxs.count()
    //
    //    val filteredWxsCount =
    //      wxs.filter{ case tuple@(_, _, otherCoverage) => otherCoverage.getOrElse(0l) >= rnaDepth &&
    //                                                      passesWxsThresholds(tuple) }
    //        .count()
    //
    //    val tn = rnaCount - filteredRnaCount + wxsCount - filteredWxsCount
    //
    //    val specificity = tn / (rnaCount + wxsCount).toFloat
    //
    //    val sensitivity = tp / both.count().toFloat

  }


}
