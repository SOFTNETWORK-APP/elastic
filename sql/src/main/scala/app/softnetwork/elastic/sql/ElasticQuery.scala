package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.SearchRequest
import com.sksamuel.elastic4s.searches.aggs.Aggregation

/** Created by smanciot on 27/06/2018.
  */
object ElasticQuery {

  import SQLImplicits._

  def select(sqlQuery: SQLQuery): Option[ElasticSelect] = {
    val select: Option[SQLSelectQuery] = sqlQuery.query
    select.map(s =>
      ElasticSelect(
        s.select.fields,
        s.select.except,
        s.sources,
        s.where.flatMap(_.criteria),
        s.limit.map(_.limit),
        s.searchRequest,
        s.aggregations
      )
    )
  }

  def aggregate(sqlQuery: SQLQuery): Seq[ElasticAggregation] = {
    val select: Option[SQLSelectQuery] = sqlQuery.query
    aggregate(select)
  }

  private[this] def aggregate(select: Option[SQLSelectQuery]): Seq[ElasticAggregation] = {
    select match {
      case Some(s: SQLSelectAggregatesQuery) =>
        s.selectAggregates.aggregates.map((sqlAggregate: SQLAggregate) => {
          val aggregation = sqlAggregate.asAggregation()

          val queryFiltered = s.where.map(_.asQuery()).getOrElse(matchAllQuery)

          aggregation.copy(
            sources = s.sources,
            query = Some(
              (sqlAggregate.function match {
                case Count if aggregation.sourceField.equalsIgnoreCase("_id") =>
                  SearchBodyBuilderFn(
                    search("") query {
                      queryFiltered
                    }
                  )
                case _ =>
                  SearchBodyBuilderFn(
                    search("") query {
                      queryFiltered
                    }
                    aggregations {
                      aggregation.agg
                    }
                    size 0
                  )
              }).string().replace("\"version\":true,", "") /*FIXME*/
            )
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
  sources: Seq[String] = Seq.empty,
  query: Option[String] = None,
  distinct: Boolean = false,
  nested: Boolean = false,
  filtered: Boolean = false,
  aggType: AggregateFunction,
  agg: Option[Aggregation] = None
)

case class ElasticSelect(
  fields: Seq[SQLField],
  except: Option[SQLExcept],
  sources: Seq[String],
  criteria: Option[SQLCriteria],
  limit: Option[Int],
  search: SearchRequest,
  aggregates: Seq[ElasticAggregation] = Seq.empty
) {
  def minScore(score: Double): ElasticSelect = {
    this.copy(search = search minScore score)
  }

  def query: String =
    SearchBodyBuilderFn(search).string().replace("\"version\":true,", "") /*FIXME*/
}
