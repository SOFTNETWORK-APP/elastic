package app.softnetwork.elastic.sql

case object LIMIT extends SQLExpr("limit")

case class SQLLimit(limit: Int) extends SQLExpr(s"limit $limit")
