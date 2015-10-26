package org.tmoerman.adam.fx.filter

import java.util.UUID

import org.bdgenomics.adam.rdd.features.{FeatureParser, BEDParser}
import org.bdgenomics.formats.avro.{Contig, Feature}
import org.tmoerman.adam.fx.BaseSparkContextSpec
import scala.collection.JavaConversions._

/**
 * @author Thomas Moerman
 */
class AluRepeatSpikeSpec extends BaseSparkContextSpec {

  val wd = "/Users/tmo/Work/exascience/cellpass/bed/"

  val radarAluBed = wd + "RADAR_Alu_hg19_chr.bed"

  val truXBed = wd + "TruSeq_Exome_b37.bed"

  "loading the bed" should "work" in {

    val parser = new BEDParser {

      override def parse(line: String) = if (line.startsWith("#")) Seq() else super.parse(line)

    }

    val radarAluRDD = sc.textFile(radarAluBed).flatMap(new MinimalBEDParser().parse)
    //val radarAluRDD = sc.textFile(radarAluBed).flatMap(parser.parse)

    //val truXRDD = sc.loadBED(truXBed)

    println(radarAluRDD.take(3).map(_.getAttributes.toMap).mkString("\n"))

  }

}

class MinimalBEDParser extends FeatureParser {

  override def parse(line: String): Seq[Feature] = {

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

    val otherFields: Map[String, String] =
      (4 to fields.length)
        .map(_.toString)
        .zip(fields.drop(3))
        .toMap

    fb.setAttributes(otherFields)

    Seq(fb.build())
  }

}