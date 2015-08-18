package org.exascience.effects

import java.util.{ArrayList, List => JList}

import htsjdk.variant.variantcontext.{VariantContext}
import htsjdk.variant.vcf.{VCFConstants, VCFHeaderLineType, VCFInfoHeaderLine}
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.bdgenomics.adam.converters.AttrKey
import org.exascience.formats.avro.SnpEffAnnotations
import scala.collection.JavaConverters._

/**
 * @author Thomas Moerman
 */
object SnpEffAnnotationConverter extends Serializable {

  private val allBetweenBracketsRegex = "\\((.*?)\\)".r

  def removeParentheses(s: String): String = allBetweenBracketsRegex.findFirstMatchIn(s).map(_.group(1)).getOrElse(s)

  def splitAtPipeSymbols(s: String): List[String] = s.split("\\|").toList.filter(! _.isEmpty)

  def cleanAndSplit = removeParentheses _ andThen splitAtPipeSymbols

  val ANN_COLUMNS = List("", "")

  private def annParser(attr: Object): Object = attr match {
    case s: String        => List(splitAtPipeSymbols(s)).asJava
    case l: JList[String] => l.asScala.toList.map(splitAtPipeSymbols(_).asJava).asJava
  }

  private def lofParser(attr: Object): JList[JList[String]] = attr match {
    case s: String        => List(cleanAndSplit(s).asJava).asJava
    case l: JList[String] => l.asScala.toList.map(cleanAndSplit(_).asJava).asJava
  }

  private def nmdParser(attr: Object): Object = attr match {
    case s: String        => List(cleanAndSplit(s).asJava).asJava
    case l: JList[String] => l.asScala.toList.map(cleanAndSplit(_).asJava).asJava
  }

  val SNP_EFF_INFO_KEYS: Seq[AttrKey] = Seq(
    AttrKey("annotations",          annParser _, new VCFInfoHeaderLine("ANN", 1, VCFHeaderLineType.String, "snpEff ANN INFO field")),
    AttrKey("lossOfFunction",       lofParser _, new VCFInfoHeaderLine("LOF", 1, VCFHeaderLineType.String, "snpEff LOF INFO field")),
    AttrKey("nonsenseMediateDecay", nmdParser _, new VCFInfoHeaderLine("NMD", 1, VCFHeaderLineType.String, "snpEff NMD INFO field")))

  lazy val VCF2SnpEffAnnotations: Map[String, (Int, Object => Object)] =
    createFieldMap(SNP_EFF_INFO_KEYS, SnpEffAnnotations.getClassSchema)

  private def createFieldMap(keys: Seq[AttrKey], schema: Schema): Map[String, (Int, Object => Object)] = {
    keys.filter(_.attrConverter != null).map(field => {
      val avroField = schema.getField(field.adamKey)
      field.vcfKey -> (avroField.pos, field.attrConverter)
    })(collection.breakOut)
  }

  private def fillRecord[T <% SpecificRecord](fieldMap: Map[String, (Int, Object => Object)], vc: VariantContext, record: T): T = {
    for ((v, a) <- fieldMap) {
      val attr = vc.getAttribute(v)
      if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
        record.put(a._1, a._2(attr))
      }
    }
    record
  }

  def convert(vc: VariantContext, annotation: SnpEffAnnotations): SnpEffAnnotations = {
    fillRecord(VCF2SnpEffAnnotations, vc, annotation)
  }

}