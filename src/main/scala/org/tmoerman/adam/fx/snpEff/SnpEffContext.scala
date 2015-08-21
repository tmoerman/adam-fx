package org.tmoerman.adam.fx.snpeff

import org.apache.hadoop.io.LongWritable
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.misc.HadoopUtil
import org.seqdoop.hadoop_bam.{VCFInputFormat, VariantContextWritable}
import org.tmoerman.adam.fx.avro._
import org.tmoerman.adam.fx.snpeff.model.VariantContextWithSnpEffAnnotations

/**
 * @author Thomas Moerman
 */
object SnpEffContext {

  implicit def sparkContextToEffectsContext(sc: SparkContext): SnpEffContext = new SnpEffContext(sc)

}

class SnpEffContext(val sc: SparkContext) extends Serializable with Logging {

  private def loadVariantsFromFile(filePath: String): RDD[(LongWritable, VariantContextWritable)] = {
    val job = HadoopUtil.newJob(sc)

    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job))

    if (Metrics.isRecording) records.instrument() else records

    records
  }

  def loadVariantsWithSnpEffAnnotations(filePath: String, sd: Option[SequenceDictionary] = None): RDD[VariantContextWithSnpEffAnnotations] = {
    val vcc    = new VariantContextConverter(sd)
    val vcc4fx = new VariantContextConverterForSnpEff(vcc, sd)

    loadVariantsFromFile(filePath).flatMap(pair => vcc4fx.convertToVariantsWithSnpEffAnnotations(pair._2.get))
  }

  def loadSnpEffAnnotations(filePath: String, sd: Option[SequenceDictionary] = None): RDD[SnpEffAnnotations] = {
    val vcc    = new VariantContextConverter(sd)
    val vcc4fx = new VariantContextConverterForSnpEff(vcc, sd)

    loadVariantsFromFile(filePath).map(pair => vcc4fx.convertToSnpEffAnnotations(pair._2.get))
  }

}