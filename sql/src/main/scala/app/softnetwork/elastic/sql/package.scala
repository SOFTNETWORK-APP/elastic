package app.softnetwork.elastic

import java.util.regex.Pattern

import scala.reflect.runtime.universe._

import scala.util.Try

/** Created by smanciot on 27/06/2018.
  */
package object sql {

  import scala.language.implicitConversions

  implicit def asString(token: Option[_ <: SQLToken]): String = token match {
    case Some(t) => t.sql
    case _       => ""
  }

  sealed trait SQLToken extends Serializable {
    def sql: String
    override def toString: String = sql
  }

  abstract class SQLExpr(override val sql: String) extends SQLToken

  case object SELECT extends SQLExpr("select")
  case object FILTER extends SQLExpr("filter")
  case object FROM extends SQLExpr("from")
  case object WHERE extends SQLExpr("where")
  case object LIMIT extends SQLExpr("limit")

  case class SQLLimit(limit: Int) extends SQLExpr(s"limit $limit")

  case class SQLIdentifier(
    identifier: String,
    alias: Option[String] = None,
    distinct: Option[String] = None
  ) extends SQLExpr(
        if (alias.isDefined)
          s"${distinct.getOrElse("")} ${alias.get}.$identifier".trim
        else
          s"${distinct.getOrElse("")} $identifier".trim
      )
      with SQLSource {
    lazy val nested: Boolean = identifier.contains('.') && !identifier.endsWith(".raw")
  }

  abstract class SQLValue[+T](val value: T)(implicit ev$1: T => Ordered[T]) extends SQLToken {
    def choose[R >: T](
      values: Seq[R],
      operator: Option[SQLExpressionOperator],
      separator: String = "|"
    )(implicit ev: R => Ordered[R]): Option[R] = {
      if (values.isEmpty)
        None
      else
        operator match {
          case Some(_: EQ.type) => values.find(_ == value)
          case Some(_: NE.type) => values.find(_ != value)
          case Some(_: GE.type) => values.filter(_ >= value).sorted.reverse.headOption
          case Some(_: GT.type) => values.filter(_ > value).sorted.reverse.headOption
          case Some(_: LE.type) => values.filter(_ <= value).sorted.headOption
          case Some(_: LT.type) => values.filter(_ < value).sorted.headOption
          case _                => values.headOption
        }
    }
  }

  case class SQLBoolean(value: Boolean) extends SQLToken {
    override def sql: String = s"$value"
  }

  case class SQLLiteral(override val value: String) extends SQLValue[String](value) {
    override def sql: String = s""""$value""""
    import SQLImplicits._
    private lazy val pattern: Pattern = value.pattern
    def like: Seq[String] => Boolean = {
      _.exists { pattern.matcher(_).matches() }
    }
    def eq: Seq[String] => Boolean = {
      _.exists { _.contentEquals(value) }
    }
    def ne: Seq[String] => Boolean = {
      _.forall { !_.contentEquals(value) }
    }
    override def choose[R >: String](
      values: Seq[R],
      operator: Option[SQLExpressionOperator],
      separator: String = "|"
    )(implicit ev: R => Ordered[R]): Option[R] = {
      operator match {
        case Some(_: EQ.type)   => values.find(v => v.toString contentEquals value)
        case Some(_: NE.type)   => values.find(v => !(v.toString contentEquals value))
        case Some(_: LIKE.type) => values.find(v => pattern.matcher(v.toString).matches())
        case None               => Some(values.mkString(separator))
        case _                  => super.choose(values, operator, separator)
      }
    }
  }

  abstract class SQLNumeric[+T](override val value: T)(implicit ev$1: T => Ordered[T])
      extends SQLValue[T](value) {
    override def sql: String = s"$value"
    override def choose[R >: T](
      values: Seq[R],
      operator: Option[SQLExpressionOperator],
      separator: String = "|"
    )(implicit ev: R => Ordered[R]): Option[R] = {
      operator match {
        case None => if (values.isEmpty) None else Some(values.max)
        case _    => super.choose(values, operator, separator)
      }
    }
  }

  case class SQLInt(override val value: Int) extends SQLNumeric[Int](value) {
    def max: Seq[Int] => Int = x => Try(x.max).getOrElse(0)
    def min: Seq[Int] => Int = x => Try(x.min).getOrElse(0)
    def eq: Seq[Int] => Boolean = {
      _.exists { _ == value }
    }
    def ne: Seq[Int] => Boolean = {
      _.forall { _ != value }
    }
  }

  case class SQLDouble(override val value: Double) extends SQLNumeric[Double](value) {
    def max: Seq[Double] => Double = x => Try(x.max).getOrElse(0)
    def min: Seq[Double] => Double = x => Try(x.min).getOrElse(0)
    def eq: Seq[Double] => Boolean = {
      _.exists { _ == value }
    }
    def ne: Seq[Double] => Boolean = {
      _.forall { _ != value }
    }
  }

  sealed abstract class SQLValues[+R: TypeTag, +T <: SQLValue[R]](val values: Seq[T])
      extends SQLToken {
    override def sql = s"(${values.map(_.sql).mkString(",")})"
    lazy val innerValues: Seq[R] = values.map(_.value)
  }

  case class SQLLiteralValues(override val values: Seq[SQLLiteral])
      extends SQLValues[String, SQLValue[String]](values) {
    def eq: Seq[String] => Boolean = {
      _.exists { s => innerValues.exists(_.contentEquals(s)) }
    }
    def ne: Seq[String] => Boolean = {
      _.forall { s => innerValues.forall(!_.contentEquals(s)) }
    }
  }

  case class SQLNumericValues[R: TypeTag](override val values: Seq[SQLNumeric[R]])
      extends SQLValues[R, SQLNumeric[R]](values) {
    def eq: Seq[R] => Boolean = {
      _.exists { n => innerValues.contains(n) }
    }
    def ne: Seq[R] => Boolean = {
      _.forall { n => !innerValues.contains(n) }
    }
  }

  sealed trait SQLOperator extends SQLToken

  sealed trait SQLExpressionOperator extends SQLOperator

  case object EQ extends SQLExpr("=") with SQLExpressionOperator
  case object GE extends SQLExpr(">=") with SQLExpressionOperator
  case object GT extends SQLExpr(">") with SQLExpressionOperator
  case object IN extends SQLExpr("in") with SQLExpressionOperator
  case object LE extends SQLExpr("<=") with SQLExpressionOperator
  case object LIKE extends SQLExpr("like") with SQLExpressionOperator
  case object LT extends SQLExpr("<") with SQLExpressionOperator
  case object NE extends SQLExpr("<>") with SQLExpressionOperator
  case object BETWEEN extends SQLExpr("between") with SQLExpressionOperator
  case object IS_NULL extends SQLExpr("is null") with SQLExpressionOperator
  case object IS_NOT_NULL extends SQLExpr("is not null") with SQLExpressionOperator

  sealed trait SQLPredicateOperator extends SQLOperator

  case object AND extends SQLPredicateOperator { override val sql: String = "and" }
  case object OR extends SQLPredicateOperator { override val sql: String = "or" }
  case object NOT extends SQLPredicateOperator { override val sql: String = "not" }

  sealed trait SQLCriteria extends SQLToken {
    def operator: SQLOperator
  }

  case class SQLExpression(
    columnName: SQLIdentifier,
    operator: SQLExpressionOperator,
    value: SQLToken
  ) extends SQLCriteria {
    override def sql = s"$columnName ${operator.sql} $value"
  }

  case class SQLIsNull(columnName: SQLIdentifier) extends SQLCriteria {
    override val operator: SQLOperator = IS_NULL
    override def sql = s"$columnName ${operator.sql}"
  }

  case class SQLIsNotNull(columnName: SQLIdentifier) extends SQLCriteria {
    override val operator: SQLOperator = IS_NOT_NULL
    override def sql = s"$columnName ${operator.sql}"
  }

  case class SQLIn[R, +T <: SQLValue[R]](
    columnName: SQLIdentifier,
    values: SQLValues[R, T],
    not: Option[NOT.type] = None
  ) extends SQLCriteria {
    override def sql =
      s"$columnName ${not.map(_ => "not ").getOrElse("")}${operator.sql} ${values.sql}"
    override def operator: SQLOperator = IN
  }

  case class SQLBetween(columnName: SQLIdentifier, from: SQLLiteral, to: SQLLiteral)
      extends SQLCriteria {
    override def sql = s"$columnName ${operator.sql} ${from.sql} and ${to.sql}"
    override def operator: SQLOperator = BETWEEN
  }

  case class ElasticGeoDistance(
    columnName: SQLIdentifier,
    distance: SQLLiteral,
    lat: SQLDouble,
    lon: SQLDouble
  ) extends SQLCriteria {
    override def sql = s"${operator.sql}($columnName,(${lat.sql},${lon.sql})) <= ${distance.sql}"
    override def operator: SQLOperator = SQLDistance
  }

  case class SQLPredicate(
    leftCriteria: SQLCriteria,
    operator: SQLPredicateOperator,
    rightCriteria: SQLCriteria,
    not: Option[NOT.type] = None
  ) extends SQLCriteria {
    val leftParentheses: Boolean = leftCriteria match {
      case _: ElasticRelation => false
      case _                  => true
    }
    val rightParentheses: Boolean = rightCriteria match {
      case _: ElasticRelation => false
      case _                  => true
    }
    override def sql = s"${if (leftParentheses) s"(${leftCriteria.sql})"
    else leftCriteria.sql} ${operator.sql}${not
      .map(_ => " not")
      .getOrElse("")} ${if (rightParentheses) s"(${rightCriteria.sql})" else rightCriteria.sql}"
  }

  sealed trait ElasticOperator extends SQLOperator
  case object NESTED extends SQLExpr("nested") with ElasticOperator
  case object CHILD extends SQLExpr("child") with ElasticOperator
  case object PARENT extends SQLExpr("parent") with ElasticOperator

  sealed abstract class ElasticRelation(val criteria: SQLCriteria, val operator: ElasticOperator)
      extends SQLCriteria {
    override def sql = s"${operator.sql}(${criteria.sql})"
    def _retrieveType(criteria: SQLCriteria): Option[String] = criteria match {
      case SQLPredicate(left, _, _, _) => _retrieveType(left)
      case SQLBetween(col, _, _)       => Some(col.identifier.split("\\.").head)
      case SQLExpression(col, _, _)    => Some(col.identifier.split("\\.").head)
      case SQLIn(col, _, _)            => Some(col.identifier.split("\\.").head)
      case SQLIsNull(col)              => Some(col.identifier.split("\\.").head)
      case SQLIsNotNull(col)           => Some(col.identifier.split("\\.").head)
      case relation: ElasticRelation   => relation.`type`
      case _                           => None
    }
    lazy val `type`: Option[String] = _retrieveType(criteria)
  }

  case class ElasticNested(override val criteria: SQLCriteria)
      extends ElasticRelation(criteria, NESTED)

  case class ElasticChild(override val criteria: SQLCriteria)
      extends ElasticRelation(criteria, CHILD)

  case class ElasticParent(override val criteria: SQLCriteria)
      extends ElasticRelation(criteria, PARENT)

  sealed trait SQLDelimiter extends SQLToken
  trait StartDelimiter extends SQLDelimiter
  trait EndDelimiter extends SQLDelimiter
  case object StartPredicate extends SQLExpr("(") with StartDelimiter
  case object EndPredicate extends SQLExpr(")") with EndDelimiter
  case object Separator extends SQLExpr(",") with EndDelimiter

  def choose[T](
    values: Seq[T],
    criteria: Option[SQLCriteria],
    function: Option[SQLFunction] = None
  )(implicit ev$1: T => Ordered[T]): Option[T] = {
    criteria match {
      case Some(SQLExpression(_, operator, value: SQLValue[T] @unchecked)) =>
        value.choose[T](values, Some(operator))
      case _ =>
        function match {
          case Some(_: SQLMin.type) => Some(values.min)
          case Some(_: SQLMax.type) => Some(values.max)
          // FIXME        case Some(_: SQLSum.type) => Some(values.sum)
          // FIXME        case Some(_: SQLAvg.type) => Some(values.sum / values.length  )
          case _ => values.headOption
        }
    }
  }

  def toRegex(value: String): String = {
    val startWith = value.startsWith("%")
    val endWith = value.endsWith("%")
    val v =
      if (startWith && endWith)
        value.substring(1, value.length - 1)
      else if (startWith)
        value.substring(1)
      else if (endWith)
        value.substring(0, value.length - 1)
      else
        value
    s"""${if (startWith) ".*?"}$v${if (endWith) ".*?"}"""
  }

  case class SQLAlias(alias: String) extends SQLExpr(s" as $alias")

  sealed trait SQLFunction extends SQLToken
  case object SQLCount extends SQLExpr("count") with SQLFunction
  case object SQLMin extends SQLExpr("min") with SQLFunction
  case object SQLMax extends SQLExpr("max") with SQLFunction
  case object SQLAvg extends SQLExpr("avg") with SQLFunction
  case object SQLSum extends SQLExpr("sum") with SQLFunction
  case object SQLDistance extends SQLExpr("distance") with SQLFunction with SQLOperator

  case class SQLField(
    func: Option[SQLFunction] = None,
    identifier: SQLIdentifier,
    alias: Option[SQLAlias] = None
  ) extends SQLToken {
    override def sql: String =
      func match {
        case Some(f) => s"${f.sql}(${identifier.sql})${asString(alias)}"
        case _       => s"${identifier.sql}${asString(alias)}"
      }
  }

  class SQLCountField(
    override val identifier: SQLIdentifier,
    override val alias: Option[SQLAlias] = None,
    val filter: Option[SQLFilter] = None
  ) extends SQLField(Some(SQLCount), identifier, alias)

  case class SQLSelect(fields: Seq[SQLField] = Seq(SQLField(identifier = SQLIdentifier("*"))))
      extends SQLToken {
    override def sql: String = s"$SELECT ${fields.map(_.sql).mkString(",")}"
  }

  class SQLSelectCount(
    val countFields: Seq[SQLCountField] = Seq(new SQLCountField(identifier = SQLIdentifier("*")))
  ) extends SQLSelect(countFields)

  sealed trait SQLSource extends SQLToken

  case class SQLTable(source: SQLSource, alias: Option[SQLAlias] = None) extends SQLToken {
    override def sql: String = s"$source${asString(alias)}"
  }

  case class SQLFrom(tables: Seq[SQLTable]) extends SQLToken {
    override def sql: String = s" $FROM ${tables.map(_.sql).mkString(",")}"
  }

  case class SQLWhere(criteria: Option[SQLCriteria]) extends SQLToken {
    override def sql: String = criteria match {
      case Some(c) => s" $WHERE ${c.sql}"
      case _       => ""
    }
  }

  case class SQLFilter(criteria: Option[SQLCriteria]) extends SQLToken {
    override def sql: String = criteria match {
      case Some(c) => s" $FILTER($c)"
      case _       => ""
    }
  }

  case class SQLSelectQuery(
    select: SQLSelect = SQLSelect(),
    from: SQLFrom,
    where: Option[SQLWhere],
    limit: Option[SQLLimit] = None
  ) extends SQLToken {
    override def sql: String = s"${select.sql}${from.sql}${asString(where)}${asString(limit)}"
  }

  class SQLCountQuery(
    val selectCount: SQLSelectCount = new SQLSelectCount(),
    from: SQLFrom,
    where: Option[SQLWhere],
    limit: Option[SQLLimit] = None
  ) extends SQLSelectQuery(selectCount, from, where)

  case class SQLQuery(query: String)

  case class SQLQueries(queries: List[SQLQuery])
}
