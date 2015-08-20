package org.exascience.effects

import org.scalatest.{Matchers, FlatSpec}
import SnpEffAnnotationsParser._

/**
 * @author Thomas Moerman
 */
class SnpEffAnnotationConverterSpec extends FlatSpec with Matchers {

  "splitAtPipeSymbols" should "return a List containing a null value when the input is empty" in {
    splitAtPipeSymbols("") shouldBe List(null)
  }

  "splitAtPipeSymbols" should "return a list when the input is a simple string" in {
    splitAtPipeSymbols("foo") shouldBe List("foo")
  }

  "splitAtPipeSymbols" should "correctly split a string delimited by pipe symbols" in {
    splitAtPipeSymbols("foo|gee|bar") shouldBe List("foo", "gee", "bar")
  }

  "splitAtPipeSymbols" should "yield null for empty positions" in {
    splitAtPipeSymbols("||") shouldBe List(null, null, null)

    splitAtPipeSymbols("foo||bar") shouldBe List("foo", null, "bar")
    splitAtPipeSymbols("foo||") shouldBe List("foo", null, null)

    splitAtPipeSymbols("|gee|bar") shouldBe List(null, "gee", "bar")
    splitAtPipeSymbols("||bar") shouldBe List(null, null, "bar")

    splitAtPipeSymbols("foo|gee|") shouldBe List("foo", "gee", null)
    splitAtPipeSymbols("|gee|") shouldBe List(null, "gee", null)
  }

  "splitAtAmpersand" should "work in the trivial case" in {
    splitAtAmpersand("foo") shouldBe List("foo")

    splitAtAmpersand("foo&gee") shouldBe List("foo", "gee")
  }

  "cleanParentheses" should "return the text verbatim when there are no enclosing brackets" in {
    removeParentheses("foo") should be ("foo")
  }

  "cleanAndSplit" should "implement the composition" in {
    cleanAndSplit("(foo|gee|bar)") should be (List("foo", "gee", "bar"))
  }

  "pattern matching an array" should "work" in {
    val a = Array("foo", "gee", "bar", "boz") match {
      case Array(foo, gee, _*) => (foo, gee, None)
    }

    a shouldBe ("foo", "gee", None)
  }

}