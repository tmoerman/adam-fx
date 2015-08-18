package org.exascience.effects

import org.apache.hadoop.io.LongWritable
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.misc.HadoopUtil
import org.seqdoop.hadoop_bam.{VariantContextWritable, VCFInputFormat}
import org.apache.spark.rdd.MetricsContext._
import org.exascience.formats.avro._

/**
 * @author Thomas Moerman
 */
object EffectsContext {

  implicit def sparkContextToEffectsContext(sc: SparkContext): EffectsContext = new EffectsContext(sc)

}

class EffectsContext(val sc: SparkContext) extends Serializable with Logging {

  def loadSnpEffAnnotations(
    filePath: String,
    sd: Option[SequenceDictionary] = None): RDD[SnpEffAnnotations] = {

    val job = HadoopUtil.newJob(sc)
    val vcc = new VariantContextConverterForEffects(sd)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job))

    if (Metrics.isRecording) records.instrument() else records

    records.map(p => vcc.convertToSnpEffAnnotations(p._2.get))
  }

}