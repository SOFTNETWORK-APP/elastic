package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.searches.{MultiSearchRequest, SearchRequest}

/** Created by smanciot on 27/06/2018.
  */
object ElasticQuery {
  def search(request: SearchRequest): SQLSearchRequest = ???

  def multiSearch(request: MultiSearchRequest): SQLMultiSearchRequest = ???
}
