package app.softnetwork.elastic

import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.searches.queries.Query

import java.util.regex.Pattern
import scala.reflect.runtime.universe._
import scala.util.Try

/** Created by smanciot on 27/06/2018.
  */
package object sql {

  import scala.language.implicitConversions

  import SQLImplicits._

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
    columnName: String,
    alias: Option[String] = None,
    distinct: Option[String] = None,
    nested: Boolean = false
  ) extends SQLExpr({
        var parts: Seq[String] = columnName.split("\\.").toSeq
        alias match {
          case Some(a) => parts = a +: parts
          case _       =>
        }
        s"${distinct.getOrElse("")} ${parts.mkString(".")}".trim
      })
      with SQLSource {

    lazy val nestedType: Option[String] = if (nested) Some(columnName.split('.').head) else None

    lazy val innerHitsName: Option[String] = if (nested) alias else None

    def update(query: SQLSelectQuery): SQLIdentifier = {
      val parts: Seq[String] = columnName.split("\\.").toSeq
      if (query.aliases.contains(parts.head)) {
        query.unnests.find(_._1 == parts.head) match {
          case Some(tuple) =>
            this.copy(
              alias = Some(parts.head),
              columnName = s"${tuple._2}.${parts.tail.mkString(".")}",
              nested = true
            )
          case _ =>
            this.copy(
              alias = Some(parts.head),
              columnName = parts.tail.mkString(".")
            )
        }
      } else {
        this
      }
    }
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

  sealed trait SQLPredicateOperator extends SQLLogicalOperator

  case object AND extends SQLPredicateOperator { override val sql: String = "and" }
  case object OR extends SQLPredicateOperator { override val sql: String = "or" }
  case object NOT extends SQLLogicalOperator { override val sql: String = "not" }

  sealed trait ElasticFilter {
    def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query
  }

  case class ElasticBoolQuery(
    var innerFilters: Seq[ElasticFilter] = Nil,
    var mustFilters: Seq[ElasticFilter] = Nil,
    var notFilters: Seq[ElasticFilter] = Nil,
    var shouldFilters: Seq[ElasticFilter] = Nil,
    group: Boolean = false
  ) extends ElasticFilter {
    def filter(filter: ElasticFilter): ElasticBoolQuery = {
      if (filter != this)
        innerFilters = filter +: innerFilters
      this
    }

    def must(filter: ElasticFilter): ElasticBoolQuery = {
      if (filter != this)
        mustFilters = filter +: mustFilters
      this
    }

    def not(filter: ElasticFilter): ElasticBoolQuery = {
      if (filter != this)
        notFilters = filter +: notFilters
      this
    }
    def should(filter: ElasticFilter): ElasticBoolQuery = {
      if (filter != this)
        shouldFilters = filter +: shouldFilters
      this
    }

    def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      import com.sksamuel.elastic4s.ElasticApi._
      bool(
        mustFilters.map(_.query(innerHitsNames, currentQuery)),
        shouldFilters.map(_.query(innerHitsNames, currentQuery)),
        notFilters.map(_.query(innerHitsNames, currentQuery))
      )
        .filter(innerFilters.map(_.query(innerHitsNames, currentQuery)))
    }

  }

  sealed trait SQLCriteria extends SQLToken {
    def operator: SQLOperator

    def nested: Boolean = false

    def update(query: SQLSelectQuery): SQLCriteria

    def group: Boolean

    lazy val boolQuery: ElasticBoolQuery = ElasticBoolQuery()

    def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter

    def asBoolQuery(currentQuery: Option[ElasticBoolQuery]): ElasticBoolQuery = {
      currentQuery match {
        case Some(q) if q.group && !group => q
        case _                            => boolQuery
      }
    }

    def filter(currentQuery: Option[ElasticBoolQuery]): ElasticBoolQuery = {
      val query = asBoolQuery(currentQuery)
      query.filter(this.asFilter(Option(query)))
    }

  }

  sealed trait SQLCriteriaWithIdentifier extends SQLCriteria {
    def identifier: SQLIdentifier
    override def nested: Boolean = identifier.nested
    override def group: Boolean = false
  }

  case class SQLExpression(
    identifier: SQLIdentifier,
    operator: SQLExpressionOperator,
    value: SQLToken,
    maybeNot: Option[NOT.type] = None
  ) extends SQLCriteriaWithIdentifier
      with ElasticFilter {
    override def sql =
      s"$identifier ${maybeNot.map(_ => "not ").getOrElse("")}${operator.sql} $value"
    override def update(query: SQLSelectQuery): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(query))
      if (updated.nested) {
        ElasticNested(updated)
      } else
        updated
    }

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      value match {
        case n: SQLNumeric[Any] @unchecked =>
          operator match {
            case _: GE.type =>
              maybeNot match {
                case Some(_) =>
                  rangeQuery(identifier.columnName) lt n.sql
                case _ =>
                  rangeQuery(identifier.columnName) gte n.sql
              }
            case _: GT.type =>
              maybeNot match {
                case Some(_) =>
                  rangeQuery(identifier.columnName) lte n.sql
                case _ =>
                  rangeQuery(identifier.columnName) gt n.sql
              }
            case _: LE.type =>
              maybeNot match {
                case Some(_) =>
                  rangeQuery(identifier.columnName) gt n.sql
                case _ =>
                  rangeQuery(identifier.columnName) lte n.sql
              }
            case _: LT.type =>
              maybeNot match {
                case Some(_) =>
                  rangeQuery(identifier.columnName) gte n.sql
                case _ =>
                  rangeQuery(identifier.columnName) lt n.sql
              }
            case _: EQ.type =>
              maybeNot match {
                case Some(_) =>
                  not(termQuery(identifier.columnName, n.sql))
                case _ =>
                  termQuery(identifier.columnName, n.sql)
              }
            case _: NE.type =>
              maybeNot match {
                case Some(_) =>
                  termQuery(identifier.columnName, n.sql)
                case _ =>
                  not(termQuery(identifier.columnName, n.sql))
              }
            case _ => matchAllQuery
          }
        case l: SQLLiteral =>
          operator match {
            case _: LIKE.type =>
              maybeNot match {
                case Some(_) =>
                  not(regexQuery(identifier.columnName, toRegex(l.value)))
                case _ =>
                  regexQuery(identifier.columnName, toRegex(l.value))
              }
            case _: GE.type =>
              maybeNot match {
                case Some(_) =>
                  rangeQuery(identifier.columnName) lt l.value
                case _ =>
                  rangeQuery(identifier.columnName) gte l.value
              }
            case _: GT.type =>
              maybeNot match {
                case Some(_) =>
                  rangeQuery(identifier.columnName) lte l.value
                case _ =>
                  rangeQuery(identifier.columnName) gt l.value
              }
            case _: LE.type =>
              maybeNot match {
                case Some(_) =>
                  rangeQuery(identifier.columnName) gt l.value
                case _ =>
                  rangeQuery(identifier.columnName) lte l.value
              }
            case _: LT.type =>
              maybeNot match {
                case Some(_) =>
                  rangeQuery(identifier.columnName) gte l.value
                case _ =>
                  rangeQuery(identifier.columnName) lt l.value
              }
            case _: EQ.type =>
              maybeNot match {
                case Some(_) =>
                  not(termQuery(identifier.columnName, l.value))
                case _ =>
                  termQuery(identifier.columnName, l.value)
              }
            case _: NE.type =>
              maybeNot match {
                case Some(_) =>
                  termQuery(identifier.columnName, l.value)
                case _ =>
                  not(termQuery(identifier.columnName, l.value))
              }
            case _ => matchAllQuery
          }
        case b: SQLBoolean =>
          operator match {
            case _: EQ.type =>
              termQuery(identifier.columnName, b.value)
            case _: NE.type =>
              not(termQuery(identifier.columnName, b.value))
            case _ => matchAllQuery
          }
        case _ => matchAllQuery
      }
    }
  }

  case class SQLIsNull(identifier: SQLIdentifier)
      extends SQLCriteriaWithIdentifier
      with ElasticFilter {
    override val operator: SQLOperator = IS_NULL
    override def sql = s"$identifier ${operator.sql}"
    override def update(query: SQLSelectQuery): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(query))
      if (updated.nested) {
        ElasticNested(updated)
      } else
        updated
    }

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      not(existsQuery(identifier.columnName))
    }
  }

  case class SQLIsNotNull(identifier: SQLIdentifier)
      extends SQLCriteriaWithIdentifier
      with ElasticFilter {
    override val operator: SQLOperator = IS_NOT_NULL
    override def sql = s"$identifier ${operator.sql}"
    override def update(query: SQLSelectQuery): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(query))
      if (updated.nested) {
        ElasticNested(updated)
      } else
        updated
    }

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      existsQuery(identifier.columnName)
    }
  }

  case class SQLIn[R, +T <: SQLValue[R]](
    identifier: SQLIdentifier,
    values: SQLValues[R, T],
    maybeNot: Option[NOT.type] = None
  ) extends SQLCriteriaWithIdentifier
      with ElasticFilter {
    override def sql =
      s"$identifier ${maybeNot.map(_ => "not ").getOrElse("")}${operator.sql} ${values.sql}"
    override def operator: SQLOperator = IN
    override def update(query: SQLSelectQuery): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(query))
      if (updated.nested) {
        ElasticNested(updated)
      } else
        updated
    }

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      val _values: Seq[Any] = values.innerValues
      val t =
        _values.headOption match {
          case Some(_: Double) =>
            termsQuery(identifier.columnName, _values.asInstanceOf[Seq[Double]])
          case Some(_: Integer) =>
            termsQuery(identifier.columnName, _values.asInstanceOf[Seq[Integer]])
          case Some(_: Long) =>
            termsQuery(identifier.columnName, _values.asInstanceOf[Seq[Long]])
          case _ => termsQuery(identifier.columnName, _values.map(_.toString))
        }
      maybeNot match {
        case Some(_) => not(t)
        case _       => t
      }
    }
  }

  case class SQLBetween(identifier: SQLIdentifier, from: SQLLiteral, to: SQLLiteral)
      extends SQLCriteriaWithIdentifier
      with ElasticFilter {
    override def sql = s"$identifier ${operator.sql} ${from.sql} and ${to.sql}"
    override def operator: SQLOperator = BETWEEN
    override def update(query: SQLSelectQuery): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(query))
      if (updated.nested) {
        ElasticNested(updated)
      } else
        updated
    }

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      rangeQuery(identifier.columnName) gte from.value lte to.value
    }
  }

  case class ElasticGeoDistance(
    identifier: SQLIdentifier,
    distance: SQLLiteral,
    lat: SQLDouble,
    lon: SQLDouble
  ) extends SQLCriteriaWithIdentifier
      with ElasticFilter {
    override def sql = s"${operator.sql}($identifier,(${lat.sql},${lon.sql})) <= ${distance.sql}"
    override def operator: SQLOperator = SQLDistance
    override def update(query: SQLSelectQuery): ElasticGeoDistance =
      this.copy(identifier = identifier.update(query))

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      geoDistanceQuery(identifier.columnName).point(lat.value, lon.value) distance distance.value
    }
  }

  case class SQLPredicate(
    leftCriteria: SQLCriteria,
    operator: SQLPredicateOperator,
    rightCriteria: SQLCriteria,
    not: Option[NOT.type] = None,
    group: Boolean = false
  ) extends SQLCriteria {
    override def sql = s"${if (group) s"(${leftCriteria.sql}"
    else leftCriteria.sql} ${operator.sql}${not
      .map(_ => " not")
      .getOrElse("")} ${if (group) s"${rightCriteria.sql})" else rightCriteria.sql}"
    override def update(query: SQLSelectQuery): SQLCriteria = {
      val updatedPredicate = this.copy(
        leftCriteria = leftCriteria.update(query),
        rightCriteria = rightCriteria.update(query)
      )
      if (updatedPredicate.nested) {
        ElasticNested(unnest(updatedPredicate))
      } else
        updatedPredicate
    }

    private[this] def unnest(criteria: SQLCriteria): SQLCriteria = criteria match {
      case p: SQLPredicate =>
        p.copy(
          leftCriteria = unnest(p.leftCriteria),
          rightCriteria = unnest(p.rightCriteria)
        )
      case r: ElasticNested => r.criteria
      case _                => criteria
    }

    override lazy val boolQuery: ElasticBoolQuery = ElasticBoolQuery(group = group)

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = {
      val query = asBoolQuery(currentQuery)
      operator match {
        case AND =>
          (not match {
            case Some(_) => query.not(rightCriteria.asFilter(Option(query)))
            case _       => query.filter(rightCriteria.asFilter(Option(query)))
          }).filter(leftCriteria.asFilter(Option(query)))
        case OR =>
          (not match {
            case Some(_) => query.not(rightCriteria.asFilter(Option(query)))
            case _       => query.should(rightCriteria.asFilter(Option(query)))
          }).should(leftCriteria.asFilter(Option(query)))
      }
    }

    override def nested: Boolean = leftCriteria.nested && rightCriteria.nested
  }

  sealed trait ElasticOperator extends SQLOperator
  case object NESTED extends SQLExpr("nested") with ElasticOperator
  case object CHILD extends SQLExpr("child") with ElasticOperator
  case object PARENT extends SQLExpr("parent") with ElasticOperator
  case object MATCH extends SQLExpr("match") with ElasticOperator

  sealed abstract class ElasticRelation(val criteria: SQLCriteria, val operator: ElasticOperator)
      extends SQLCriteria
      with ElasticFilter {
    override def sql = s"${operator.sql}(${criteria.sql})"

    private[this] def rtype(criteria: SQLCriteria): Option[String] = criteria match {
      case SQLPredicate(left, _, right, _, _) => rtype(left).orElse(rtype(right))
      case c: SQLCriteriaWithIdentifier =>
        c.identifier.nestedType.orElse(c.identifier.columnName.split('.').headOption)
      case relation: ElasticRelation => relation.relationType
      case _                         => None
    }

    lazy val relationType: Option[String] = rtype(criteria)

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

    override def group: Boolean = criteria.group

  }

  case class ElasticNested(override val criteria: SQLCriteria)
      extends ElasticRelation(criteria, NESTED) {
    override def update(query: SQLSelectQuery): ElasticNested =
      this.copy(criteria = criteria.update(query))

    override def nested: Boolean = true

    private[this] def name(criteria: SQLCriteria): Option[String] = criteria match {
      case SQLPredicate(left, _, right, _, _) => name(left).orElse(name(right))
      case c: SQLCriteriaWithIdentifier =>
        c.identifier.innerHitsName.orElse(c.identifier.columnName.split('.').headOption)
      case n: ElasticNested => name(n.criteria)
      case _                => None
    }

    lazy val innerHitsName: Option[String] = name(criteria)

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      if (innerHitsNames.contains(innerHitsName.getOrElse(""))) {
        criteria.asFilter(currentQuery).query(innerHitsNames, currentQuery)
      } else {
        val boolQuery = Option(ElasticBoolQuery(group = true))
        nestedQuery(
          relationType.getOrElse(""),
          criteria
            .asFilter(boolQuery)
            .query(innerHitsNames + innerHitsName.getOrElse(""), boolQuery)
        ) /*.scoreMode(ScoreMode.None)*/
          .inner(innerHits(innerHitsName.getOrElse("")))
      }
    }
  }

  case class ElasticChild(override val criteria: SQLCriteria)
      extends ElasticRelation(criteria, CHILD) {
    override def update(query: SQLSelectQuery): ElasticChild =
      this.copy(criteria = criteria.update(query))

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query =
      hasChildQuery(
        relationType.getOrElse(""),
        criteria.asFilter(currentQuery).query(innerHitsNames, currentQuery)
      )
  }

  case class ElasticParent(override val criteria: SQLCriteria)
      extends ElasticRelation(criteria, PARENT) {
    override def update(query: SQLSelectQuery): ElasticParent =
      this.copy(criteria = criteria.update(query))

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query =
      hasParentQuery(
        relationType.getOrElse(""),
        criteria.asFilter(currentQuery).query(innerHitsNames, currentQuery),
        score = false
      )
  }

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
      case Some(SQLExpression(_, operator, value: SQLValue[T] @unchecked, _)) =>
        value.choose[T](values, Some(operator))
      case _ =>
        function match {
          case Some(_: Min.type) => Some(values.min)
          case Some(_: Max.type) => Some(values.max)
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
  sealed trait AggregateFunction extends SQLFunction
  case object Count extends SQLExpr("count") with AggregateFunction
  case object Min extends SQLExpr("min") with AggregateFunction
  case object Max extends SQLExpr("max") with AggregateFunction
  case object Avg extends SQLExpr("avg") with AggregateFunction
  case object Sum extends SQLExpr("sum") with AggregateFunction
  case object SQLDistance extends SQLExpr("distance") with SQLFunction with SQLOperator

  case class SQLField(
    identifier: SQLIdentifier,
    alias: Option[SQLAlias] = None
  ) extends SQLToken {
    override def sql: String = s"${identifier.sql}${asString(alias)}"
    def update(query: SQLSelectQuery): SQLField = this.copy(identifier = identifier.update(query))
    lazy val sourceField: String =
      if (identifier.nested) {
        identifier.alias
          .map(a => s"$a.")
          .getOrElse("") + identifier.columnName.split("\\.").tail.mkString(".")
      } else {
        identifier.columnName
      }
  }

  case class ElasticMatch(
    identifier: SQLIdentifier,
    value: SQLLiteral,
    options: Option[String]
  ) extends SQLCriteriaWithIdentifier
      with ElasticFilter {
    override def sql: String =
      s"${operator.sql}(${identifier.sql},${value.sql}${options.map(o => s""","$o"""").getOrElse("")})"
    override def operator: SQLOperator = MATCH
    override def update(query: SQLSelectQuery): SQLCriteria =
      this.copy(identifier = identifier.update(query))

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      matchQuery(identifier.columnName, value.value)
    }
  }

  case class SQLExcept(fields: Seq[SQLField]) extends SQLToken {
    override def sql: String = s" except(${fields.map(_.sql).mkString(",")})"
    def update(query: SQLSelectQuery): SQLExcept =
      this.copy(fields = fields.map(_.update(query)))
  }

  class SQLAggregate(
    val function: AggregateFunction,
    override val identifier: SQLIdentifier,
    override val alias: Option[SQLAlias] = None,
    val filter: Option[SQLFilter] = None
  ) extends SQLField(identifier, alias) {
    override def sql: String = s"${function.sql}(${identifier.sql})${asString(alias)}"
    override def update(query: SQLSelectQuery): SQLAggregate =
      new SQLAggregate(function, identifier.update(query), alias, filter.map(_.update(query)))
  }

  case class SQLSelect(
    fields: Seq[SQLField] = Seq(SQLField(identifier = SQLIdentifier("*"))),
    except: Option[SQLExcept] = None
  ) extends SQLToken {
    override def sql: String =
      s"$SELECT ${fields.map(_.sql).mkString(",")}${except.map(_.sql).getOrElse("")}"
    def update(query: SQLSelectQuery): SQLSelect =
      this.copy(fields = fields.map(_.update(query)), except = except.map(_.update(query)))
  }

  class SQLSelectAggregates(
    val aggregates: Seq[SQLAggregate]
  ) extends SQLSelect(aggregates) {
    override def update(query: SQLSelectQuery): SQLSelectAggregates = new SQLSelectAggregates(
      aggregates.map(_.update(query))
    )
  }

  sealed trait SQLSource extends SQLToken {
    def update(query: SQLSelectQuery): SQLSource
  }

  case class SQLUnnest(identifier: SQLIdentifier) extends SQLSource {
    override def sql: String = s"unnest(${identifier /*.copy(distinct = None)*/ .sql})"
    def update(query: SQLSelectQuery): SQLUnnest = this.copy(identifier = identifier.update(query))
  }

  case class SQLTable(source: SQLSource, alias: Option[SQLAlias] = None) extends SQLToken {
    override def sql: String = s"$source${asString(alias)}"
    def update(query: SQLSelectQuery): SQLTable = this.copy(source = source.update(query))
  }

  case class SQLFrom(tables: Seq[SQLTable]) extends SQLToken {
    override def sql: String = s" $FROM ${tables.map(_.sql).mkString(",")}"
    lazy val aliases: Seq[String] = tables.flatMap((table: SQLTable) => table.alias).map(_.alias)
    lazy val unnests: Seq[(String, String)] = tables.collect { case SQLTable(u: SQLUnnest, a) =>
      (a.map(_.alias).getOrElse(u.identifier.columnName), u.identifier.columnName)
    }
    def update(query: SQLSelectQuery): SQLFrom = this.copy(tables = tables.map(_.update(query)))
  }

  case class SQLWhere(criteria: Option[SQLCriteria]) extends SQLToken {
    override def sql: String = criteria match {
      case Some(c) => s" $WHERE ${c.sql}"
      case _       => ""
    }
    def update(query: SQLSelectQuery): SQLWhere =
      this.copy(criteria = criteria.map(_.update(query)))
  }

  case class SQLFilter(criteria: Option[SQLCriteria]) extends SQLToken {
    override def sql: String = criteria match {
      case Some(c) => s" $FILTER($c)"
      case _       => ""
    }
    def update(query: SQLSelectQuery): SQLFilter =
      this.copy(criteria = criteria.map(_.update(query)))
  }

  case class SQLSelectQuery(
    select: SQLSelect = SQLSelect(),
    from: SQLFrom,
    where: Option[SQLWhere],
    limit: Option[SQLLimit] = None
  ) extends SQLToken {
    override def sql: String = s"${select.sql}${from.sql}${asString(where)}${asString(limit)}"
    lazy val aliases: Seq[String] = from.aliases
    lazy val unnests: Seq[(String, String)] = from.unnests
    def update(): SQLSelectQuery = {
      val updated = this.copy(from = from.update(this))
      updated.copy(select = select.update(updated), where = where.map(_.update(updated)))
    }
  }

  class SQLSelectAggregatesQuery(
    val selectAggregates: SQLSelectAggregates,
    from: SQLFrom,
    where: Option[SQLWhere],
    limit: Option[SQLLimit] = None
  ) extends SQLSelectQuery(selectAggregates, from, where, limit) {
    def withFrom(from: SQLFrom): SQLSelectAggregatesQuery = new SQLSelectAggregatesQuery(
      selectAggregates,
      from,
      where,
      limit
    )
    override def update(): SQLSelectAggregatesQuery = {
      val updated = this.withFrom(from.update(this))
      new SQLSelectAggregatesQuery(
        selectAggregates.update(updated),
        updated.from,
        where.map(_.update(updated)),
        limit
      )
    }
  }

  case class SQLQuery(query: String)

  case class SQLQueries(queries: List[SQLQuery])
}
