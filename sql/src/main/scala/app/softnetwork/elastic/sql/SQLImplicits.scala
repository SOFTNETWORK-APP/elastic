package app.softnetwork.elastic.sql

//import com.sksamuel.elastic4s.requests.searches.term.{BuildableTermsQuery, TermsQuery}

import scala.util.matching.Regex

/** Created by smanciot on 27/06/2018.
  */
object SQLImplicits {
  import scala.language.implicitConversions

  implicit def queryToSQLCriteria(query: String): Option[SQLCriteria] = {
    val maybeQuery: Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = query
    maybeQuery match {
      case Some(Left(l)) => l.where.flatMap(_.criteria)
      case _             => None
    }
  }

  implicit def queryToSQLQuery(
    query: String
  ): Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = {
    SQLParser(query) match {
      case Left(_)  => None
      case Right(r) => Some(r)
    }
  }

  implicit def requestToElasticSearchRequest(request: SQLSearchRequest): ElasticSearchRequest =
    ElasticSearchRequest(
      request.select.fields,
      request.select.except,
      request.sources,
      request.where.flatMap(_.criteria),
      request.limit.map(_.limit),
      request.searchRequest,
      request.aggregations
    )

  implicit def sqllikeToRegex(value: String): Regex = toRegex(value).r

//  implicit def BuildableTermsNoOp[T]: BuildableTermsQuery[T] = new BuildableTermsQuery[T] {
//    override def build(q: TermsQuery[T]): Any = null // not used by the http builders
//  }

}
