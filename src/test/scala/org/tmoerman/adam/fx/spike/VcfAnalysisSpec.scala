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


  // ---


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


  // ---


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
      .map{ case tuple@(annotatedGenotype, _) => (uniqueVariantKey(annotatedGenotype), tuple) } // TODO two complected steps: adding a new key and collapsing the tuple
      .reduceByKey(withMaxVariantCallErrorProbability)


  val rnaMap: RDD[(UniqueVariantKey, AnnotatedGenotypeWithRPKM)] =
    rnaTmp
      .flatMap(keyedBySampleGeneKeyRna)
      .leftOuterJoin(sampleGeneKeyToRpkm)
      .map(dropKey)
      .map{ case tuple@(annotatedGenotype, _) => (uniqueVariantKey(annotatedGenotype), tuple) }
      .reduceByKey(withMaxVariantCallErrorProbability)


  // ---


  val lnOf2 = math.log(2)

  val dpStep = math.floor(math.log(math.max(rnaTmp.map(g => g.getGenotype.getReadDepth).max,
    wxsTmp.map(g => g.getGenotype.getReadDepth).max)) / lnOf2)

  val qStep = math.floor(math.log(math.max(
    rnaTmp.map(g => g.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability).max,
    wxsTmp.map(g => g.getGenotype.getVariantCallingAnnotations.getVariantCallErrorProbability).max)) / lnOf2)

  val rpkmStep =  math.floor(math.log(sampleGeneKeyToRpkm.map(dropKey).max) / lnOf2)


  // ---


  val defaultReadDepth = 10
  val defaultGenotypeQuality = 50
  val defaultQuality = 250
  val defaultRPKM = 0

  val nrSteps = 50


  // ---


  def allPassReadDepth(minReadDepth: Int)(coverages: Option[(Option[Coverage], Option[Coverage])]) =
    coverages.exists {
      case (wxsCoverage, rnaCoverage) =>
        wxsCoverage.exists(_ >= minReadDepth) &&
        rnaCoverage.exists(_ >= minReadDepth) }

  def allPassDefaultReadDepth = allPassReadDepth(defaultReadDepth) _

  val coGroup: RDD[(Option[Coverage], Option[AnnotatedGenotypeWithRPKM],
                    Option[Coverage], Option[AnnotatedGenotypeWithRPKM])] =
    wxsMap.cogroup(rnaMap)
          .map{ case (uniqueVariantKey, (wxsGenotypes, rnaGenotypes)) =>
                  (dropAlleles(uniqueVariantKey), (wxsGenotypes.headOption, rnaGenotypes.headOption)) }
          .leftOuterJoin(coveragesCogroup)
          .map(dropKey)

          // only consider entries with sufficient read depth TODO here?
          //.filter{ case (_, coverages) => allPassDefaultReadDepth(coverages) }

          // destructure coverage values
          .map{ case ((wxsGenotypeOption, rnaGenotypeOption), Some((wxsCoverage, rnaCoverage))) =>
                  (wxsCoverage, wxsGenotypeOption, rnaCoverage, rnaGenotypeOption)

                case _ => throw new IllegalArgumentException("wtf") }

          .cache()


  val CONCORDANCE = "concordance"
  val WXS_ONLY    = "wxsOnly"
  val RNA_ONLY    = "rnaOnly"

  def toCategory(opt1: Option[Any], opt2: Option[Any]): Category = (opt1, opt2) match {
    case (Some(_), Some(_)) => CONCORDANCE
    case (Some(_), None   ) => WXS_ONLY
    case (None,    Some(_)) => RNA_ONLY
    case (None,    None   ) => throw new IllegalArgumentException("cannot both be None")
  }


  def passesThresholds(depth:           Double,
                       genotypeQuality: Double,
                       quality:         Double,
                       rpkmThreshold:   Double)
                      (coverageOption: Option[Coverage],
                       genotypeOption: Option[AnnotatedGenotypeWithRPKM]): Boolean = {

    lazy val genotypePasses: Boolean =
      genotypeOption match {
        case Some((annotatedGenotype, rpkm)) =>
          val genotype = annotatedGenotype.getGenotype

          genotype.getReadDepth                                                >= depth           &&
          genotype.getGenotypeQuality                                          >= genotypeQuality &&
          genotype.getVariantCallingAnnotations.getVariantCallErrorProbability >= quality         &&
          rpkm.exists(_ >= rpkmThreshold)
        case _ => false
      }

    coverageOption.exists(_ >= depth) && genotypePasses
  }

  def toCategorySweepValues(sweepPredicates: Seq[SweepValuePredicate])
                           (wxsCoverage:       Option[Coverage],
                            wxsGenotypeOption: Option[AnnotatedGenotypeWithRPKM],
                            rnaCoverage:       Option[Coverage],
                            rnaGenotypeOption: Option[AnnotatedGenotypeWithRPKM]): Seq[(Category, SweepValue)] = {

    val category = toCategory(wxsGenotypeOption, rnaGenotypeOption)

    val passedPredicates = category match {
      case CONCORDANCE =>
        sweepPredicates
          .takeWhile { case (v, predicate) => predicate(wxsCoverage, wxsGenotypeOption) &&
                                              predicate(rnaCoverage, rnaGenotypeOption) }

      case WXS_ONLY =>
        sweepPredicates.takeWhile { case (v, predicate) => predicate(wxsCoverage, wxsGenotypeOption) }


      case RNA_ONLY =>
        sweepPredicates.takeWhile { case (v, predicate) => predicate(rnaCoverage, rnaGenotypeOption) }
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
    def f(x: SweepValue) = Seq(((CONCORDANCE, x), 0l),
                               ((WXS_ONLY,    x), 0l),
                               ((RNA_ONLY,    x), 0l))
    
    sweepValues.flatMap(f).toMap
  }

  val rpkmRange =
    toZeroCounts(rpkmSweepValues) ++
    coGroup
      .flatMap(tupled(toCategorySweepValues(rpkmSweepPredicates)))
      .countByValue()
      .toMap
      .map { case ((cat, v), count) => CountRepr(cat, v, count)}


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
