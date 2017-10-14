package com.levelmoney.scalaedn

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

private[scalaedn] class DefaultSource
  extends RelationProvider with SchemaRelationProvider {

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String])
    : BaseRelation = ednRelation(sqlContext, parameters)

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType)
    : BaseRelation = ednRelation(sqlContext, parameters, Some(schema))

  private def ednRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      maybeSchema: Option[StructType] = None)
    : EDNRelation = {

    val path = parameters.getOrElse("path",
      throw new IllegalArgumentException("Required parameter 'path' was unspecified.")
    )

    new EDNRelation(path, maybeSchema)(sqlContext)
  }
}
