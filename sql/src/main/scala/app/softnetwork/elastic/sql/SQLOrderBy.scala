package app.softnetwork.elastic.sql

case object OrderBy extends SQLExpr("order by") with SQLRegex

sealed trait SortOrder extends SQLRegex

case object Desc extends SQLExpr("desc") with SortOrder

case object Asc extends SQLExpr("asc") with SortOrder

case class SQLFieldSort(field: String, order: Option[SortOrder]) extends SQLToken {
  override def sql: String = s"$field ${order.getOrElse(Asc)}"
}

case class SQLOrderBy(sorts: Seq[SQLFieldSort]) extends SQLToken {
  override def sql: String = s" $OrderBy ${sorts.mkString(",")}"
}
