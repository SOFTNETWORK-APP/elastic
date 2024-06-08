package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.queries.{BoolQuery, Query}

/** Created by smanciot on 27/06/2018.
  */
object ElasticQuery {

  import ElasticFilters._
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

        val fields = s.select.fields.map(_.identifier.identifier)

        val sources = s.from.tables.map((table: SQLTable) => table.source.sql)

        val queryFiltered = filter(criteria) match {
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

        Some(ElasticSelect(s.select.fields, sources, q.replace("\"version\":true,", "") /*FIXME*/ ))

      case _ => None
    }
  }

  def count(sqlQuery: SQLQuery): Seq[ElasticCount] = {
    val select: Option[SQLSelectQuery] = sqlQuery.query
    count(select)
  }

  private[this] def count(select: Option[SQLSelectQuery]): Seq[ElasticCount] = {
    select match {
      case Some(s: SQLCountQuery) =>
        val criteria = s.where match {
          case Some(w) => w.criteria
          case _       => None
        }
        val sources = s.from.tables.map((table: SQLTable) => table.source.sql)
        s.selectCount.countFields.map((countField: SQLCountField) => {
          val sourceField = countField.identifier.identifier

          val field = countField.alias match {
            case Some(alias) => alias.alias
            case _           => sourceField
          }

          val distinct = countField.identifier.distinct.isDefined

          val filtered = countField.filter

          val isFiltered = filtered.isDefined

          val nested = sourceField.contains(".")

          val agg =
            if (distinct)
              s"agg_distinct_${sourceField.replace(".", "_")}"
            else
              s"agg_${sourceField.replace(".", "_")}"

          var aggPath = Seq[String]()

          val queryFiltered = filter(criteria) match {
            case b: BoolQuery => b
            case q: Query     => boolQuery().filter(q)
          }

          val q =
            if (sourceField.equalsIgnoreCase("_id")) { // "native" elastic count
              SearchBodyBuilderFn(
                search("") query {
                  queryFiltered
                }
              ).string()
            } else {
              val _agg =
                if (distinct)
                  cardinalityAgg(agg, sourceField)
                else
                  valueCountAgg(agg, sourceField)

              def _filtered = {
                if (isFiltered) {
                  val filteredAgg = s"filtered_agg"
                  aggPath ++= Seq(filteredAgg)
                  filterAgg(filteredAgg, filter(filtered.get.criteria)) subaggs {
                    aggPath ++= Seq(agg)
                    _agg
                  }
                } else {
                  aggPath ++= Seq(agg)
                  _agg
                }
              }

              SearchBodyBuilderFn(
                search("") query {
                  queryFiltered
                }
                aggregations {
                  if (nested) {
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

          ElasticCount(
            aggPath.mkString("."),
            field,
            sourceField,
            sources,
            q.replace("\"version\":true,", ""), /*FIXME*/
            distinct,
            nested,
            isFiltered
          )
        })
      case _ => Seq.empty
    }
  }

}

case class ElasticCount(
  agg: String,
  field: String,
  sourceField: String,
  sources: Seq[String],
  query: String,
  distinct: Boolean = false,
  nested: Boolean = false,
  filtered: Boolean = false
)

case class ElasticSelect(
  fields: Seq[SQLField],
  sources: Seq[String],
  query: String
)
