package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.requests.searches.{MultiSearchBuilderFn, MultiSearchRequest}

case class ElasticMultiSearchRequest(
  requests: Seq[ElasticSearchRequest],
  multiSearch: MultiSearchRequest
) {
  def query: String = MultiSearchBuilderFn(multiSearch).replace("\"version\":true,", "") /*FIXME*/
}
