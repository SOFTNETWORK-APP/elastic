package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.aggs.Aggregation
import com.sksamuel.elastic4s.searches.queries.{BoolQuery, Query}

/** Created by smanciot on 27/06/2018.
  */
object ElasticQuery {

  import SQLImplicits._

  def select(sqlQuery: SQLQuery): Option[ElasticSelect] = select(sqlQuery.query)

  private[this] def select(query: String): Option[ElasticSelect] = {
    val select: Option[SQLSelectQuery] = query
    select match {

      case Some(s) =>
        val criteria = s.where match {
          case Some(w) => w.criteria
          case _       => None
        }

        val fields = s.select.fields.map(_.sourceField)

        val sources = s.from.tables.map((table: SQLTable) => table.source.sql)

        val queryFiltered = criteria
          .map(_.filter(None).query(Set.empty))
          .getOrElse(matchAllQuery) /*filter(criteria)*/ match {
          case b: BoolQuery => b
          case q: Query     => boolQuery().filter(q)
        }

        var _search = search("") query {
          queryFiltered
        } sourceInclude fields

        _search = s.limit match {
          case Some(l) => _search limit l.limit from 0
          case _       => _search
        }

        val q = SearchBodyBuilderFn(_search).string()

        Some(
          ElasticSelect(
            s.select.fields,
            sources,
            s.where.flatMap(_.criteria),
            s.limit.map(_.limit),
            q.replace("\"version\":true,", "") /*FIXME*/
          )
        )

      case _ => None
    }
  }

  def aggregate(sqlQuery: SQLQuery): Seq[ElasticAggregation] = {
    val select: Option[SQLSelectQuery] = sqlQuery.query
    aggregate(select)
  }

  private[this] def aggregate(select: Option[SQLSelectQuery]): Seq[ElasticAggregation] = {
    select match {
      case Some(s: SQLSelectAggregatesQuery) =>
        val criteria = s.where match {
          case Some(w) => w.criteria
          case _       => None
        }
        val sources = s.from.tables.collect { case SQLTable(source: SQLIdentifier, _) =>
          source.sql
        }
        s.selectAggregates.aggregates.map((aggregation: SQLAggregate) => {
          val identifier = aggregation.identifier
          val sourceField = identifier.columnName

          val field = aggregation.alias match {
            case Some(alias) => alias.alias
            case _           => sourceField
          }

          val distinct = identifier.distinct.isDefined

          val filtered = aggregation.filter

          val isFiltered = filtered.isDefined

          val agg =
            if (distinct)
              s"agg_distinct_${sourceField.replace(".", "_")}"
            else
              s"agg_${sourceField.replace(".", "_")}"

          var aggPath = Seq[String]()

          val queryFiltered = criteria
            .map(_.filter(None).query(Set.empty))
            .getOrElse(matchAllQuery) /*filter(criteria)*/ match {
            case b: BoolQuery => b
            case q: Query     => boolQuery().filter(q)
          }

          val q = {
            aggregation.function match {
              case Count if sourceField.equalsIgnoreCase("_id") =>
                SearchBodyBuilderFn(
                  search("") query {
                    queryFiltered
                  }
                ).string()
              case other => {
                val _agg = {
                  other match {
                    case Count =>
                      if (distinct)
                        cardinalityAgg(agg, sourceField)
                      else {
                        valueCountAgg(agg, sourceField)
                      }
                    case Min => minAgg(agg, sourceField)
                    case Max => maxAgg(agg, sourceField)
                    case Avg => avgAgg(agg, sourceField)
                    case Sum => sumAgg(agg, sourceField)
                  }
                }

                def _filtered: Aggregation = {
                  aggregation.filter match {
                    case Some(f) =>
                      val filteredAgg = s"filtered_agg"
                      aggPath ++= Seq(filteredAgg)
                      filterAgg(
                        filteredAgg,
                        f.criteria
                          .map(_.asFilter().query(Set(identifier.innerHitsName).flatten))
                          .getOrElse(matchAllQuery())
                      ) subaggs {
                        aggPath ++= Seq(agg)
                        _agg
                      }
                    case _ =>
                      aggPath ++= Seq(agg)
                      _agg
                  }
                }

                SearchBodyBuilderFn(
                  search("") query {
                    queryFiltered
                  }
                  aggregations {
                    if (identifier.nested) {
                      val path = sourceField.split("\\.").head
                      val nestedAgg = s"nested_$path"
                      aggPath ++= Seq(nestedAgg)
                      nestedAggregation(nestedAgg, path) subaggs {
                        _filtered
                      }
                    } else {
                      _filtered
                    }
                  }
                  size 0
                ).string()
              }
            }
          }

          ElasticAggregation(
            aggPath.mkString("."),
            field,
            sourceField,
            sources,
            q.replace("\"version\":true,", ""), /*FIXME*/
            distinct,
            identifier.nested,
            isFiltered,
            aggregation.function
          )
        })
      case _ => Seq.empty
    }
  }

}

case class ElasticAggregation(
  aggName: String,
  field: String,
  sourceField: String,
  sources: Seq[String],
  query: String,
  distinct: Boolean = false,
  nested: Boolean = false,
  filtered: Boolean = false,
  aggType: AggregateFunction
)

case class ElasticSelect(
  fields: Seq[SQLField],
  sources: Seq[String],
  criteria: Option[SQLCriteria],
  limit: Option[Int],
  query: String
)
