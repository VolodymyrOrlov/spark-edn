package com.levelmoney.scalaedn

import java.text.DateFormat
import java.util.UUID

import scala.util.parsing.combinator.JavaTokenParsers

private[scalaedn] object Parser extends JavaTokenParsers {
  val set: Parser[Set[Any]] = "#{" ~> rep(elem) <~ "}" ^^ (Set() ++ _)
  val map: Parser[Map[Any, Any]] = "{" ~> rep(pair) <~ "}" ^^ (Map() ++ _)
  val vector: Parser[Vector[Any]] = "[" ~> rep(elem) <~ "]" ^^ (Vector() ++ _)
  val list: Parser[List[Any]] = "(" ~> rep(elem) <~ ")"
  val keyword: Parser[String] = """:[^,#\{\}\[\]\s]+""".r
  lazy val pair: Parser[(Any, Any)] = elem ~ elem ^^ {
    case key ~ value => (key, value)
  }
  lazy val tagElem: Parser[Any] = """#[^,"#\{\}\[\]\s]+""".r ~ elem ^^ {
    case "#uuid" ~ (value: String) => UUID.fromString(value.tail.init)
    case "#inst" ~ (value: String) => DateFormat.getDateInstance(DateFormat.SHORT)
      .parse(value.tail.init)
    case name ~ value => (name, value)
  }
  val ednElem: Parser[Any] =  set | map | vector | list | keyword | tagElem |
    wholeNumber ^^ (_.toLong) |
    floatingPointNumber ^^ (_.toDouble) |
    "nil"               ^^ (_ => null)  |
    "true"              ^^ (_ => true)  |
    "false"             ^^ (_ => false) |
    regexp |
    stringLiteral
  val elem: Parser[Any] = ednElem | "," ~> elem

  def regexp: Parser[String] =
    ("[#]\"([^\"]*)\"").r
  override protected val whiteSpace = """(\s|;;.*)+""".r

}
