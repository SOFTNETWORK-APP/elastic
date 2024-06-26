package app.softnetwork.elastic.sql

case object ORDER_BY extends SQLExpr("order by")

sealed trait SortOrder extends SQLToken

case object DESC extends SQLExpr("desc") with SortOrder

case object ASC extends SQLExpr("asc") with SortOrder

case class SQLFieldSort(field: String, order: Option[SortOrder]) extends SQLToken {
  override def sql: String = s"$field ${order.getOrElse(ASC).sql}"
}

case class SQLOrderBy(sorts: Seq[SQLFieldSort]) extends SQLToken {
  override def sql: String = s" $ORDER_BY ${sorts.map(_.sql).mkString(",")}"
}
