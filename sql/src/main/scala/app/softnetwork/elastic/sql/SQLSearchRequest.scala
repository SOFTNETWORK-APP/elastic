package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi.{matchAllQuery, search}
import com.sksamuel.elastic4s.searches.SearchRequest
import com.sksamuel.elastic4s.searches.sort.FieldSort

case class SQLSearchRequest(
  select: SQLSelect = SQLSelect(),
  from: SQLFrom,
  where: Option[SQLWhere],
  orderBy: Option[SQLOrderBy] = None,
  limit: Option[SQLLimit] = None
) extends SQLToken {
  override def sql: String =
    s"${select.sql}${from.sql}${asString(where)}${asString(orderBy)}${asString(limit)}"
  lazy val aliases: Seq[String] = from.aliases
  lazy val unnests: Seq[(String, String, Option[SQLLimit])] = from.unnests
  def update(): SQLSearchRequest = {
    val updated = this.copy(from = from.update(this))
    updated.copy(select = select.update(updated), where = where.map(_.update(updated)))
  }

  lazy val fields: Seq[String] =
    select.fields
      .filterNot {
        case _: SQLAggregate => true
        case _               => false
      }
      .map(_.sourceField)

  lazy val aggregates: Seq[SQLAggregate] = select.fields.collect { case a: SQLAggregate => a }

  lazy val aggregations: Seq[ElasticAggregation] = aggregates.map(_.asAggregation())

  lazy val excludes: Seq[String] = select.except.map(_.fields.map(_.sourceField)).getOrElse(Nil)

  lazy val sources: Seq[String] = from.tables.collect { case SQLTable(source: SQLIdentifier, _) =>
    source.sql
  }

  lazy val searchRequest: SearchRequest = {
    var _search: SearchRequest = search("") query {
      where.map(_.asQuery()).getOrElse(matchAllQuery)
    } sourceInclude fields

    _search = excludes match {
      case Nil      => _search
      case excludes => _search sourceExclude excludes
    }

    _search = aggregations match {
      case Nil => _search
      case _   => _search aggregations { aggregations.map(_.agg) }
    }

    _search = orderBy match {
      case Some(o) =>
        _search sortBy o.sorts.map(sort =>
          sort.order match {
            case Some(DESC) => FieldSort(sort.field).desc()
            case _          => FieldSort(sort.field).asc()
          }
        )
      case _ => _search
    }

    if (aggregations.nonEmpty && fields.isEmpty) {
      _search size 0
    } else {
      limit match {
        case Some(l) => _search limit l.limit from 0
        case _       => _search
      }
    }
  }
}
