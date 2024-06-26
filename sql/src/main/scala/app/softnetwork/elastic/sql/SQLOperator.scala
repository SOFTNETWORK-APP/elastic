package app.softnetwork.elastic.sql

trait SQLOperator extends SQLToken

sealed trait SQLExpressionOperator extends SQLOperator

sealed trait SQLComparisonOperator extends SQLExpressionOperator

case object Eq extends SQLExpr("=") with SQLComparisonOperator
case object Ne extends SQLExpr("<>") with SQLComparisonOperator
case object Ge extends SQLExpr(">=") with SQLComparisonOperator
case object Gt extends SQLExpr(">") with SQLComparisonOperator
case object Le extends SQLExpr("<=") with SQLComparisonOperator
case object Lt extends SQLExpr("<") with SQLComparisonOperator

sealed trait SQLLogicalOperator extends SQLExpressionOperator with SQLRegex

case object In extends SQLExpr("in") with SQLLogicalOperator
case object Like extends SQLExpr("like") with SQLLogicalOperator
case object Between extends SQLExpr("between") with SQLLogicalOperator
case object IsNull extends SQLExpr("is null") with SQLLogicalOperator
case object IsNotNull extends SQLExpr("is not null") with SQLLogicalOperator
case object Not extends SQLExpr("not") with SQLLogicalOperator

sealed trait SQLPredicateOperator extends SQLLogicalOperator

case object And extends SQLExpr("and") with SQLPredicateOperator
case object Or extends SQLExpr("or") with SQLPredicateOperator

case object Union extends SQLExpr("union") with SQLOperator with SQLRegex

sealed trait ElasticOperator extends SQLOperator with SQLRegex
case object Nested extends SQLExpr("nested") with ElasticOperator
case object Child extends SQLExpr("child") with ElasticOperator
case object Parent extends SQLExpr("parent") with ElasticOperator
case object Match extends SQLExpr("match") with ElasticOperator
