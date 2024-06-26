package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.http.search.{MultiSearchBuilderFn, SearchBodyBuilderFn}
import com.sksamuel.elastic4s.searches.{MultiSearchRequest, SearchRequest}
import com.sksamuel.elastic4s.searches.aggs.Aggregation

/** Created by smanciot on 27/06/2018.
  */
object ElasticQuery {}

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
  agg: Aggregation
)

case class ElasticSearchRequest(
  fields: Seq[SQLField],
  except: Option[SQLExcept],
  sources: Seq[String],
  criteria: Option[SQLCriteria],
  limit: Option[Int],
  search: SearchRequest,
  aggregations: Seq[ElasticAggregation] = Seq.empty
) {
  def minScore(score: Option[Double]): ElasticSearchRequest = {
    score match {
      case Some(s) => this.copy(search = search minScore s)
      case _       => this
    }
  }

  def query: String =
    SearchBodyBuilderFn(search).string().replace("\"version\":true,", "") /*FIXME*/
}

case class ElasticMultiSearchRequest(
  requests: Seq[ElasticSearchRequest],
  multiSearch: MultiSearchRequest
) {
  def query: String = MultiSearchBuilderFn(multiSearch).replace("\"version\":true,", "") /*FIXME*/
}
