package org.tmoerman.adam.fx.snpeff

import org.apache.avro.Schema
import org.apache.hadoop.io.LongWritable
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.misc.HadoopUtil
import org.seqdoop.hadoop_bam.{VCFInputFormat, VariantContextWritable}
import org.tmoerman.adam.fx.avro._
import org.tmoerman.adam.fx.snpeff.model.{AnnotatedGenotypeRDDFunctions, AnnotatedVariantRDDFunctions}

/**
 * @author Thomas Moerman
 */
object SnpEffContext {

  implicit def toSnpEffContext(sc: SparkContext): SnpEffContext = new SnpEffContext(sc)

  implicit def rddToAnnotatedVariantRDD(rdd: RDD[AnnotatedVariant]): AnnotatedVariantRDDFunctions =
    new AnnotatedVariantRDDFunctions(rdd)

  implicit def rddToAnnotatedGenotypesRDD(rdd: RDD[AnnotatedGenotype]): AnnotatedGenotypeRDDFunctions =
    new AnnotatedGenotypeRDDFunctions(rdd)

}

class SnpEffContext(val sc: SparkContext) extends Serializable with Logging {
  import SnpEffAnnotationsConverter._
  
  private def loadVariantContextsFromFile(
      filePath: String): RDD[(LongWritable, VariantContextWritable)] = {

    val job = HadoopUtil.newJob(sc)

    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job))

    if (Metrics.isRecording) records.instrument() else records

    records
  }

  /**
   * @param filePath
   *                 Either a .vcf file or a Parquet file, for which the convention is the ".adam" suffix.
   * @param predicate
   * @param projection
   * @param sd
   * @return
   */
  def loadAnnotatedVariants(
      filePath: String,
      predicate: Option[FilterPredicate] = None,
      projection: Option[Schema] = None,
      sd: Option[SequenceDictionary] = None): RDD[AnnotatedVariant] = {

    if (filePath.endsWith(".adam")) {
      sc.loadParquet[AnnotatedVariant](filePath, predicate, projection)
    } else {
      val vcc = new VariantContextConverter(sd)

      loadVariantContextsFromFile(filePath).flatMap{case (_, vcw) => vcc.toAnnotatedVariants(vcw.get())}
    }
  }

  /**
   * @param filePath
   *                 Either a .vcf file or a Parquet file, for which the convention is the ".adam" suffix.
   * @param predicate
   * @param projection
   * @param sd
   * @return
   */
  def loadAnnotatedGenotypes(
      filePath: String,
      predicate: Option[FilterPredicate] = None,
      projection: Option[Schema] = None,
      sd: Option[SequenceDictionary] = None): RDD[AnnotatedGenotype] = {

    if (filePath.endsWith(".adam")) {
      sc.loadParquet[AnnotatedGenotype](filePath, predicate, projection)
    } else {
      val vcc = new VariantContextConverter(sd)

      loadVariantContextsFromFile(filePath).flatMap{case (_, vcw) => vcc.toAnnotatedGenotypes(vcw.get())}
    }
  }

}