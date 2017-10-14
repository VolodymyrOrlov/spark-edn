package com.levelmoney.scalaedn

import java.io.StringReader

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types.StructType
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

private[scalaedn] case class EDNRelation(path: String,
                                         maybeSchema: Option[StructType])
                                        (@transient val sqlContext: SQLContext) extends BaseRelation with PrunedScan {

  @transient private lazy val logger = LogManager.getLogger(this.getClass)

  val files = sqlContext.sparkSession.sparkContext.wholeTextFiles(path)

  private val df = {

    val json = files.map {
      case (f, data) =>
        logger.info(s"Parsing [$f]")
        Parser.parseAll(Parser.ednElem, new StringReader(data)) match {
          case Parser.Success(v: Any, _) => {
            compact(render( toJson(v)))
          }
          case Parser.NoSuccess(msg: String, _) => throw new IllegalStateException(s"Could not parse EDN from [$f], $msg")
        }
    }

    maybeSchema.map(schema => sqlContext.read.schema(schema).json(json)).getOrElse(sqlContext.read.json(json))

  }

  override def schema: StructType = df.schema

  private def toJson(v: Any): JValue = v match {
    case m: Map[_, _] => m.map{case (k, v) => k.toString -> toJson(v)}
    case v: Vector[_] => v.map(toJson)
    case s: Set[_] => s.map(toJson)
    case l: List[_] => l.map(toJson)
    case n: String => n.replaceAll("[#]?\"(.+)\"", "$1");
    case b: Boolean => b
    case d: Double => d
    case l: Long => l
    case _ => null
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    df.select(requiredColumns.map(new Column(_)): _*).rdd
  }
}
