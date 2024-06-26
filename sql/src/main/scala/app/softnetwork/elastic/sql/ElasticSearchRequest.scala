package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.SearchRequest

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
