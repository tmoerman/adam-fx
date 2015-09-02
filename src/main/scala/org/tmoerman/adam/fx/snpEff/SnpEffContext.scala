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
import org.tmoerman.adam.fx.snpeff.model.VariantContextWithSnpEffAnnotations

/**
 * @author Thomas Moerman
 */
object SnpEffContext {

  implicit def sparkContextToSnpEffContext(sc: SparkContext): SnpEffContext = new SnpEffContext(sc)

  implicit def implicitSnpEffInspections(rdd: RDD[VariantContextWithSnpEffAnnotations]): SnpEffInspections = new SnpEffInspections(rdd)

}

class SnpEffContext(val sc: SparkContext) extends Serializable with Logging {
  
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

  private def loadParquetSnpEffAnnotations(
      filePath: String,
      predicate: Option[FilterPredicate] = None,
      projection: Option[Schema] = None): RDD[SnpEffAnnotations] = {

    sc.loadParquet[SnpEffAnnotations](filePath, predicate, projection)
  }

  /**
   * @param filePath
   *                 Either a .vcf file or a Parquet file, for which the convention is the ".adam" suffix.
   * @param predicate
   * @param projection
   * @param sd
   * @return
   */
  def loadVariantsWithSnpEffAnnotations(
      filePath: String,
      predicate: Option[FilterPredicate] = None,
      projection: Option[Schema] = None,
      sd: Option[SequenceDictionary] = None): RDD[VariantContextWithSnpEffAnnotations] = {

    val vcc    = new VariantContextConverter(sd)
    val vcc4fx = new VariantContextConverterForSnpEff(vcc, sd)

    if (filePath.endsWith(".adam")) {
      loadParquetSnpEffAnnotations(filePath, predicate, projection).map(a => VariantContextWithSnpEffAnnotations(a))
    } else {
      loadVariantContextsFromFile(filePath).flatMap(pair => vcc4fx.convertToVariantsWithSnpEffAnnotations(pair._2.get))
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
  def loadSnpEffAnnotations(
      filePath: String,
      predicate: Option[FilterPredicate] = None,
      projection: Option[Schema] = None,
      sd: Option[SequenceDictionary] = None): RDD[SnpEffAnnotations] = {

    val vcc    = new VariantContextConverter(sd)
    val vcc4fx = new VariantContextConverterForSnpEff(vcc, sd)

    if (filePath.endsWith(".adam")) {
      loadParquetSnpEffAnnotations(filePath, predicate, projection)
    } else {
      loadVariantContextsFromFile(filePath).map(pair => vcc4fx.convertToSnpEffAnnotations(pair._2.get))
    }
  }

}