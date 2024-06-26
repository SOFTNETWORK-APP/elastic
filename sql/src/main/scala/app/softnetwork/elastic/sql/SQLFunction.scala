package app.softnetwork.elastic.sql

sealed trait SQLFunction extends SQLToken

sealed trait AggregateFunction extends SQLFunction
case object Count extends SQLExpr("count") with AggregateFunction
case object Min extends SQLExpr("min") with AggregateFunction
case object Max extends SQLExpr("max") with AggregateFunction
case object Avg extends SQLExpr("avg") with AggregateFunction
case object Sum extends SQLExpr("sum") with AggregateFunction

case object SQLDistance extends SQLExpr("distance") with SQLFunction with SQLOperator
