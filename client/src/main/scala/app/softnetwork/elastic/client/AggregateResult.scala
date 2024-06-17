package app.softnetwork.elastic.client

import app.softnetwork.elastic.sql.AggregateFunction

sealed trait AggregateResult {
  def field: String
  def error: Option[String]
}

sealed trait MetricAgregateResult extends AggregateResult {
  def function: AggregateFunction
}

case class SingleValueAggregateResult(
  field: String,
  function: AggregateFunction,
  value: Double,
  error: Option[String] = None
) extends MetricAgregateResult
