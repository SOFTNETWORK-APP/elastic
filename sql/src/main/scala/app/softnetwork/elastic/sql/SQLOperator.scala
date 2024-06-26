package app.softnetwork.elastic.sql

trait SQLOperator extends SQLToken

sealed trait SQLExpressionOperator extends SQLOperator

sealed trait SQLComparisonOperator extends SQLExpressionOperator

case object EQ extends SQLExpr("=") with SQLComparisonOperator
case object NE extends SQLExpr("<>") with SQLComparisonOperator
case object GE extends SQLExpr(">=") with SQLComparisonOperator
case object GT extends SQLExpr(">") with SQLComparisonOperator
case object LE extends SQLExpr("<=") with SQLComparisonOperator
case object LT extends SQLExpr("<") with SQLComparisonOperator

sealed trait SQLLogicalOperator extends SQLExpressionOperator

case object IN extends SQLExpr("in") with SQLLogicalOperator
case object LIKE extends SQLExpr("like") with SQLLogicalOperator
case object BETWEEN extends SQLExpr("between") with SQLLogicalOperator
case object IS_NULL extends SQLExpr("is null") with SQLLogicalOperator
case object IS_NOT_NULL extends SQLExpr("is not null") with SQLLogicalOperator
case object NOT extends SQLLogicalOperator { override val sql: String = "not" }

sealed trait SQLPredicateOperator extends SQLLogicalOperator

case object AND extends SQLPredicateOperator { override val sql: String = "and" }
case object OR extends SQLPredicateOperator { override val sql: String = "or" }

case object UNION extends SQLExpr("union") with SQLOperator

sealed trait ElasticOperator extends SQLOperator
case object NESTED extends SQLExpr("nested") with ElasticOperator
case object CHILD extends SQLExpr("child") with ElasticOperator
case object PARENT extends SQLExpr("parent") with ElasticOperator
case object MATCH extends SQLExpr("match") with ElasticOperator
