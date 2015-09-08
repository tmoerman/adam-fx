package org.tmoerman.adam.fx.util

import org.scalatest.{FlatSpec, Matchers}
import ParseFunctions._

/**
 * @author Thomas Moerman
 */
class ParseFunctionsSpec extends FlatSpec with Matchers {

  "splitAtPipeSymbols" should "return a List containing a null value when the input is empty" in {
    splitAtPipe("") shouldBe List(null)
  }

  "splitAtPipeSymbols" should "return a list when the input is a simple string" in {
    splitAtPipe("foo") shouldBe List("foo")
  }

  "splitAtPipeSymbols" should "correctly split a string delimited by pipe symbols" in {
    splitAtPipe("foo|gee|bar") shouldBe List("foo", "gee", "bar")
  }

  "splitAtPipeSymbols" should "yield null for empty positions" in {
    splitAtPipe("||") shouldBe List(null, null, null)

    splitAtPipe("foo||bar") shouldBe List("foo", null, "bar")
    splitAtPipe("foo||") shouldBe List("foo", null, null)

    splitAtPipe("|gee|bar") shouldBe List(null, "gee", "bar")
    splitAtPipe("||bar") shouldBe List(null, null, "bar")

    splitAtPipe("foo|gee|") shouldBe List("foo", "gee", null)
    splitAtPipe("|gee|") shouldBe List(null, "gee", null)
  }

  "splitAtAmpersand" should "work in the trivial case" in {
    splitAtAmpersand("foo") shouldBe List("foo")

    splitAtAmpersand("foo&gee") shouldBe List("foo", "gee")
  }

  "cleanParentheses" should "return the text verbatim when there are no enclosing brackets" in {
    removeParentheses("foo") should be ("foo")
  }

  "cleanAndSplit" should "implement the composition" in {
    cleanAndSplitAtPipe("(foo|gee|bar)") should be (List("foo", "gee", "bar"))
  }

  "pattern matching an array" should "work" in {
    val a = Array("foo", "gee", "bar", "boz") match {
      case Array(foo, gee, _*) => (foo, gee, None)
    }

    a shouldBe ("foo", "gee", None)
  }

}