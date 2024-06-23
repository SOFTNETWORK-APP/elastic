package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.SearchRequest
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
        var _search: SearchRequest = search("") query {
          s.where.map(_.asQuery()).getOrElse(matchAllQuery)
        } sourceInclude s.fields

        val excludes = s.excludes

        excludes match {
          case Nil =>
          case _   => _search = _search sourceExclude excludes
        }

        _search = s.limit match {
          case Some(l) => _search limit l.limit from 0
          case _       => _search
        }

        Some(
          ElasticSelect(
            s.select.fields,
            s.select.except,
            s.sources,
            s.where.flatMap(_.criteria),
            s.limit.map(_.limit),
            _search
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

          val queryFiltered = criteria.map(_.asQuery()).getOrElse(matchAllQuery)

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
                      val boolQuery = Option(ElasticBoolQuery(group = true))
                      val filteredAgg = s"filtered_agg"
                      aggPath ++= Seq(filteredAgg)
                      filterAgg(
                        filteredAgg,
                        f.criteria
                          .map(
                            _.asFilter(boolQuery)
                              .query(Set(identifier.innerHitsName).flatten, boolQuery)
                          )
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
            s.sources,
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
  except: Option[SQLExcept],
  sources: Seq[String],
  criteria: Option[SQLCriteria],
  limit: Option[Int],
  search: SearchRequest
) {
  def minScore(score: Double): ElasticSelect = {
    this.copy(search = search minScore score)
  }

  def query: String =
    SearchBodyBuilderFn(search).string().replace("\"version\":true,", "") /*FIXME*/
}
