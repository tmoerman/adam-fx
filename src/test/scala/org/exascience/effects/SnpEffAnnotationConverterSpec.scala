package org.exascience.effects

import org.scalatest.{Matchers, FlatSpec}
import SnpEffAnnotationsParser._

/**
 * @author Thomas Moerman
 */
class SnpEffAnnotationConverterSpec extends FlatSpec with Matchers {

  "splitAtPipeSymbols" should "return an empty list when the input is empty" in {
    splitAtPipeSymbols("") should be (List())
  }

  "splitAtPipeSymbols" should "return a list when the input is a simple string" in {
    splitAtPipeSymbols("foo") should be (List("foo"))
  }

  "splitAtPipeSymbols" should "correctly split a string delimited by pipe symbols" in {
    splitAtPipeSymbols("foo|gee|bar") should be (List("foo", "gee", "bar"))
  }

  "cleanParentheses" should "return the text verbatim when there are no enclosing brackets" in {
    removeParentheses("foo") should be ("foo")
  }

  "cleanAndSplit" should "implement the composition" in {
    cleanAndSplit("(foo|gee|bar)") should be (List("foo", "gee", "bar"))
  }

}