package app.softnetwork.elastic.sql

import scala.util.matching.Regex

/** Created by smanciot on 27/06/2018.
  */
object SQLImplicits {
  import scala.language.implicitConversions

  implicit def queryToSQLCriteria(query: String): Option[SQLCriteria] = {
    val sql: Option[SQLSelectQuery] = query
    sql match {
      case Some(q) =>
        q.where match {
          case Some(w) => w.criteria
          case _       => None
        }
      case _ => None
    }
  }
  implicit def queryToSQLQuery(query: String): Option[SQLSelectQuery] = {
    SQLParser(query) match {
      case Left(_)  => None
      case Right(r) => Some(r)
    }
  }

  implicit def sqllikeToRegex(value: String): Regex = toRegex(value).r

}
