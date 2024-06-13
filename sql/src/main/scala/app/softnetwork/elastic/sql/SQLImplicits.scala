package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.searches.queries.term.{BuildableTermsQuery, TermsQuery}

import scala.util.matching.Regex

/** Created by smanciot on 27/06/2018.
  */
object SQLImplicits {
  import scala.language.implicitConversions

  implicit def queryToSQLCriteria(query: String): Option[SQLCriteria] = {
    val maybeQuery: Option[SQLSelectQuery] = query
    maybeQuery.flatMap(_.where.flatMap(_.criteria))
  }
  implicit def queryToSQLQuery(query: String): Option[SQLSelectQuery] = {
    SQLParser(query) match {
      case Left(_)  => None
      case Right(r) => Some(r)
    }
  }

  implicit def sqllikeToRegex(value: String): Regex = toRegex(value).r

  implicit def BuildableTermsNoOp[T]: BuildableTermsQuery[T] = new BuildableTermsQuery[T] {
    override def build(q: TermsQuery[T]): Any = null // not used by the http builders
  }

}
