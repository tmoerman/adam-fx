package org.tmoerman.adam.fx.util

import org.apache.commons.lang.StringUtils._

/**
 * @author Thomas Moerman
 */
object ParseFunctions {

  private val allBetweenBracketsRegex = "\\((.*?)\\)".r

  def removeParentheses(s: String): String = allBetweenBracketsRegex.findFirstMatchIn(s).map(_.group(1)).getOrElse(s)

  def splitAtPipe(s: String): Array[String] = s"$s ".split("\\|").map(s => if (isBlank(s)) null else s.trim)

  def splitAtAmpersand(s: String): Array[String] = s.split("\\&")

  def splitAtSemicolon(s: String): Array[String] = s.split("\\;")

  def cleanAndSplitAtPipe = removeParentheses _ andThen splitAtPipe

  def parseInt(s: String) = if (isNotEmpty(s)) Integer.valueOf(s) else null

  def parseIntPair(s: String): Option[(Integer, Integer)] =
    if (isNotEmpty(s)) {
      s.split("/") match {
        case Array(a, b) => Some((parseInt(a), parseInt(b)))
      }
    } else None

}
