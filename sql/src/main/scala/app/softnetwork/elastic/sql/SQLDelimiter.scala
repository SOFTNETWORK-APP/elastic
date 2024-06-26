package app.softnetwork.elastic.sql

sealed trait SQLDelimiter extends SQLToken

sealed trait StartDelimiter extends SQLDelimiter
case object StartPredicate extends SQLExpr("(") with StartDelimiter

sealed trait EndDelimiter extends SQLDelimiter
case object EndPredicate extends SQLExpr(")") with EndDelimiter
case object Separator extends SQLExpr(",") with EndDelimiter
