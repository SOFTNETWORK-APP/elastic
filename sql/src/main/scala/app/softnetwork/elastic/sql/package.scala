package app.softnetwork.elastic

import com.sksamuel.elastic4s.ElasticApi
import com.sksamuel.elastic4s.ElasticApi.{search, _}
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.{MultiSearchRequest, SearchRequest}
import com.sksamuel.elastic4s.searches.aggs.Aggregation
import com.sksamuel.elastic4s.searches.queries.Query
import com.sksamuel.elastic4s.searches.sort.FieldSort

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

  trait Updateable extends SQLToken {
    def update(request: SQLSearchRequest): Updateable
  }

  abstract class SQLExpr(override val sql: String) extends SQLToken

  case object SELECT extends SQLExpr("select")
  case object FILTER extends SQLExpr("filter")
  case object FROM extends SQLExpr("from")
  case object WHERE extends SQLExpr("where")
  case object LIMIT extends SQLExpr("limit")

  case object ORDER_BY extends SQLExpr("order by")
  sealed trait SortOrder extends SQLToken
  case object DESC extends SQLExpr("desc") with SortOrder
  case object ASC extends SQLExpr("asc") with SortOrder

  case class SQLLimit(limit: Int) extends SQLExpr(s"limit $limit")

  case class SQLIdentifier(
    columnName: String,
    alias: Option[String] = None,
    distinct: Option[String] = None,
    nested: Boolean = false,
    limit: Option[SQLLimit] = None
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

    def update(request: SQLSearchRequest): SQLIdentifier = {
      val parts: Seq[String] = columnName.split("\\.").toSeq
      if (request.aliases.contains(parts.head)) {
        request.unnests.find(_._1 == parts.head) match {
          case Some(tuple) =>
            this.copy(
              alias = Some(parts.head),
              columnName = s"${tuple._2}.${parts.tail.mkString(".")}",
              nested = true,
              limit = tuple._3
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

  case object UNION extends SQLExpr("union") with SQLOperator

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
    group: Boolean = false,
    filtered: Boolean = true,
    matchCriteria: Boolean = false
  ) extends ElasticFilter {
    def filter(filter: ElasticFilter): ElasticBoolQuery = {
      if (!filtered) {
        must(filter)
      } else if (filter != this)
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

    def unfilteredMatchCriteria(): ElasticBoolQuery = {
      val query = ElasticBoolQuery().copy(
        mustFilters = this.mustFilters,
        notFilters = this.notFilters,
        shouldFilters = this.shouldFilters
      )
      innerFilters.reverse.map {
        case b: ElasticBoolQuery if b.matchCriteria =>
          b.innerFilters.reverse.foreach(query.must)
          b.mustFilters.reverse.foreach(query.must)
          b.notFilters.reverse.foreach(query.not)
          b.shouldFilters.reverse.foreach(query.should)
        case filter => query.filter(filter)
      }
      query
    }

  }

  sealed trait SQLCriteria extends Updateable {
    def operator: SQLOperator

    def nested: Boolean = false

    def limit: Option[SQLLimit] = None

    def update(request: SQLSearchRequest): SQLCriteria

    def group: Boolean

    def matchCriteria: Boolean = false

    lazy val boolQuery: ElasticBoolQuery =
      ElasticBoolQuery(group = group, matchCriteria = matchCriteria)

    def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter

    def asBoolQuery(currentQuery: Option[ElasticBoolQuery]): ElasticBoolQuery = {
      currentQuery match {
        case Some(q) if q.group && !group => q
        case Some(q)                      => boolQuery.copy(filtered = q.filtered && !matchCriteria)
        case _                            => boolQuery // FIXME should never be the case
      }
    }

    def asQuery(group: Boolean = true, innerHitsNames: Set[String] = Set.empty): Query = {
      val query = boolQuery.copy(group = group)
      query
        .filter(this.asFilter(Option(query)))
        .unfilteredMatchCriteria()
        .query(innerHitsNames, Option(query))
    }

  }

  sealed trait SQLCriteriaWithIdentifier extends SQLCriteria {
    def identifier: SQLIdentifier
    override def nested: Boolean = identifier.nested
    override def group: Boolean = false
    override lazy val limit: Option[SQLLimit] = identifier.limit
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
    override def update(request: SQLSearchRequest): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(request))
      if (updated.nested) {
        ElasticNested(updated, limit)
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
              maybeNot match {
                case Some(_) =>
                  not(termQuery(identifier.columnName, b.value))
                case _ =>
                  termQuery(identifier.columnName, b.value)
              }
            case _: NE.type =>
              maybeNot match {
                case Some(_) =>
                  termQuery(identifier.columnName, b.value)
                case _ =>
                  not(termQuery(identifier.columnName, b.value))
              }
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
    override def update(request: SQLSearchRequest): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(request))
      if (updated.nested) {
        ElasticNested(updated, limit)
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
    override def update(request: SQLSearchRequest): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(request))
      if (updated.nested) {
        ElasticNested(updated, limit)
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
    override def update(request: SQLSearchRequest): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(request))
      if (updated.nested) {
        ElasticNested(updated, limit)
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
    override def update(request: SQLSearchRequest): SQLCriteria = {
      val updated = this.copy(identifier = identifier.update(request))
      if (updated.nested) {
        ElasticNested(updated, limit)
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
    override def update(request: SQLSearchRequest): ElasticGeoDistance =
      this.copy(identifier = identifier.update(request))

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
    override def update(request: SQLSearchRequest): SQLCriteria = {
      val updatedPredicate = this.copy(
        leftCriteria = leftCriteria.update(request),
        rightCriteria = rightCriteria.update(request)
      )
      if (updatedPredicate.nested) {
        val unnested = unnest(updatedPredicate)
        ElasticNested(unnested, unnested.limit)
      } else
        updatedPredicate
    }

    override lazy val limit: Option[SQLLimit] = leftCriteria.limit.orElse(rightCriteria.limit)

    private[this] def unnest(criteria: SQLCriteria): SQLCriteria = criteria match {
      case p: SQLPredicate =>
        p.copy(
          leftCriteria = unnest(p.leftCriteria),
          rightCriteria = unnest(p.rightCriteria)
        )
      case r: ElasticNested => r.criteria
      case _                => criteria
    }

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

    override def matchCriteria: Boolean = leftCriteria.matchCriteria || rightCriteria.matchCriteria
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

  case class ElasticNested(override val criteria: SQLCriteria, override val limit: Option[SQLLimit])
      extends ElasticRelation(criteria, NESTED) {
    override def update(request: SQLSearchRequest): ElasticNested =
      this.copy(criteria = criteria.update(request))

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
          .inner(
            innerHits(innerHitsName.getOrElse("")).from(0).size(limit.map(_.limit).getOrElse(3))
          )
      }
    }
  }

  case class ElasticChild(override val criteria: SQLCriteria)
      extends ElasticRelation(criteria, CHILD) {
    override def update(request: SQLSearchRequest): ElasticChild =
      this.copy(criteria = criteria.update(request))

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query =
      hasChildQuery(
        relationType.getOrElse(""),
//        criteria.asFilter(currentQuery).query(innerHitsNames, currentQuery),
        criteria.asQuery(group = group, innerHitsNames = innerHitsNames)
      )
  }

  case class ElasticParent(override val criteria: SQLCriteria)
      extends ElasticRelation(criteria, PARENT) {
    override def update(request: SQLSearchRequest): ElasticParent =
      this.copy(criteria = criteria.update(request))

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query =
      hasParentQuery(
        relationType.getOrElse(""),
//        criteria.asFilter(currentQuery).query(innerHitsNames, currentQuery),
        criteria.asQuery(group = group, innerHitsNames = innerHitsNames),
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
  ) extends Updateable {
    override def sql: String = s"${identifier.sql}${asString(alias)}"
    def update(request: SQLSearchRequest): SQLField =
      this.copy(identifier = identifier.update(request))
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
    override def update(request: SQLSearchRequest): SQLCriteria =
      this.copy(identifier = identifier.update(request))

    override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

    override def query(
      innerHitsNames: Set[String] = Set.empty,
      currentQuery: Option[ElasticBoolQuery]
    ): Query = {
      matchQuery(identifier.columnName, value.value)
    }

    override def matchCriteria: Boolean = true
  }

  case class SQLExcept(fields: Seq[SQLField]) extends Updateable {
    override def sql: String = s" except(${fields.map(_.sql).mkString(",")})"
    def update(request: SQLSearchRequest): SQLExcept =
      this.copy(fields = fields.map(_.update(request)))
  }

  class SQLAggregate(
    val function: AggregateFunction,
    override val identifier: SQLIdentifier,
    override val alias: Option[SQLAlias] = None,
    val filter: Option[SQLFilter] = None
  ) extends SQLField(identifier, alias) {
    override def sql: String = s"${function.sql}(${identifier.sql})${asString(alias)}"
    override def update(request: SQLSearchRequest): SQLAggregate =
      new SQLAggregate(function, identifier.update(request), alias, filter.map(_.update(request)))

    def asAggregation(): ElasticAggregation = {
      val sourceField = identifier.columnName

      val field = alias match {
        case Some(alias) => alias.alias
        case _           => sourceField
      }

      val distinct = identifier.distinct.isDefined

      val agg =
        if (distinct)
          s"${function.sql}_distinct_${sourceField.replace(".", "_")}"
        else
          s"${function.sql}_${sourceField.replace(".", "_")}"

      var aggPath = Seq[String]()

      val _agg =
        function match {
          case Count =>
            if (distinct)
              cardinalityAgg(agg, sourceField)
            else {
              valueCountAgg(agg, sourceField)
            }
          case Min => minAgg(agg, sourceField)
          case Max => maxAgg(agg, sourceField)
          case Avg => avgAgg(agg, sourceField)
          case Sum => sumAgg(agg, sourceField)
        }

      def _filtered: Aggregation = filter match {
        case Some(f) =>
          val boolQuery = Option(ElasticBoolQuery(group = true))
          val filteredAgg = s"filtered_agg"
          aggPath ++= Seq(filteredAgg)
          filterAgg(
            filteredAgg,
            f.criteria
              .map(
                _.asFilter(boolQuery)
                  .query(Set(identifier.innerHitsName).flatten, boolQuery)
              )
              .getOrElse(matchAllQuery())
          ) subaggs {
            aggPath ++= Seq(agg)
            _agg
          }
        case _ =>
          aggPath ++= Seq(agg)
          _agg
      }

      val aggregation =
        if (identifier.nested) {
          val path = sourceField.split("\\.").head
          val nestedAgg = s"nested_$agg"
          aggPath ++= Seq(nestedAgg)
          nestedAggregation(nestedAgg, path) subaggs {
            _filtered
          }
        } else {
          _filtered
        }

      ElasticAggregation(
        aggPath.mkString("."),
        field,
        sourceField,
        distinct = distinct,
        nested = identifier.nested,
        filtered = filter.nonEmpty,
        aggType = function,
        agg = aggregation
      )
    }
  }

  case class SQLSelect(
    fields: Seq[SQLField] = Seq(SQLField(identifier = SQLIdentifier("*"))),
    except: Option[SQLExcept] = None
  ) extends Updateable {
    override def sql: String =
      s"$SELECT ${fields.map(_.sql).mkString(",")}${except.map(_.sql).getOrElse("")}"
    def update(request: SQLSearchRequest): SQLSelect =
      this.copy(fields = fields.map(_.update(request)), except = except.map(_.update(request)))
  }

  sealed trait SQLSource extends Updateable {
    def update(request: SQLSearchRequest): SQLSource
  }

  case class SQLUnnest(identifier: SQLIdentifier, limit: Option[SQLLimit]) extends SQLSource {
    override def sql: String = s"unnest(${identifier /*.copy(distinct = None)*/ .sql})"
    def update(request: SQLSearchRequest): SQLUnnest =
      this.copy(identifier = identifier.update(request))
  }

  case class SQLTable(source: SQLSource, alias: Option[SQLAlias] = None) extends Updateable {
    override def sql: String = s"$source${asString(alias)}"
    def update(request: SQLSearchRequest): SQLTable = this.copy(source = source.update(request))
  }

  case class SQLFrom(tables: Seq[SQLTable]) extends Updateable {
    override def sql: String = s" $FROM ${tables.map(_.sql).mkString(",")}"
    lazy val aliases: Seq[String] = tables.flatMap((table: SQLTable) => table.alias).map(_.alias)
    lazy val unnests: Seq[(String, String, Option[SQLLimit])] = tables.collect {
      case SQLTable(u: SQLUnnest, a) =>
        (a.map(_.alias).getOrElse(u.identifier.columnName), u.identifier.columnName, u.limit)
    }
    def update(request: SQLSearchRequest): SQLFrom =
      this.copy(tables = tables.map(_.update(request)))
  }

  case class SQLWhere(criteria: Option[SQLCriteria]) extends Updateable {
    override def sql: String = criteria match {
      case Some(c) => s" $WHERE ${c.sql}"
      case _       => ""
    }
    def update(request: SQLSearchRequest): SQLWhere =
      this.copy(criteria = criteria.map(_.update(request)))

    def asQuery(group: Boolean = true, innerHitsNames: Set[String] = Set.empty): Query = criteria
      .map(_.asQuery(group = group, innerHitsNames = innerHitsNames))
      .getOrElse(matchAllQuery)
  }

  case class SQLFilter(criteria: Option[SQLCriteria]) extends Updateable {
    override def sql: String = criteria match {
      case Some(c) => s" $FILTER($c)"
      case _       => ""
    }
    def update(request: SQLSearchRequest): SQLFilter =
      this.copy(criteria = criteria.map(_.update(request)))
  }

  case class SQLFieldSort(field: String, order: Option[SortOrder]) extends SQLToken {
    override def sql: String = s"$field ${order.getOrElse(ASC).sql}"
  }

  case class SQLOrderBy(sorts: Seq[SQLFieldSort]) extends SQLToken {
    override def sql: String = s" $ORDER_BY ${sorts.map(_.sql).mkString(",")}"
  }

  case class SQLMultiSearchRequest(selects: Seq[SQLSearchRequest]) extends SQLToken {
    override def sql: String = s"${selects.map(_.sql).mkString(" union ")}"

    def update(): SQLMultiSearchRequest = this.copy(selects = selects.map(_.update()))

    lazy val multiSearchRequest: MultiSearchRequest = MultiSearchRequest(
      selects.map(_.searchRequest)
    )
  }

  case class SQLSearchRequest(
    select: SQLSelect = SQLSelect(),
    from: SQLFrom,
    where: Option[SQLWhere],
    orderBy: Option[SQLOrderBy] = None,
    limit: Option[SQLLimit] = None
  ) extends SQLToken {
    override def sql: String =
      s"${select.sql}${from.sql}${asString(where)}${asString(orderBy)}${asString(limit)}"
    lazy val aliases: Seq[String] = from.aliases
    lazy val unnests: Seq[(String, String, Option[SQLLimit])] = from.unnests
    def update(): SQLSearchRequest = {
      val updated = this.copy(from = from.update(this))
      updated.copy(select = select.update(updated), where = where.map(_.update(updated)))
    }

    lazy val fields: Seq[String] =
      select.fields
        .filterNot {
          case _: SQLAggregate => true
          case _               => false
        }
        .map(_.sourceField)

    lazy val aggregates: Seq[SQLAggregate] = select.fields.collect { case a: SQLAggregate => a }

    lazy val aggregations: Seq[ElasticAggregation] = aggregates.map(_.asAggregation())

    lazy val excludes: Seq[String] = select.except.map(_.fields.map(_.sourceField)).getOrElse(Nil)

    lazy val sources: Seq[String] = from.tables.collect { case SQLTable(source: SQLIdentifier, _) =>
      source.sql
    }

    lazy val searchRequest: SearchRequest = {
      var _search: SearchRequest = search("") query {
        where.map(_.asQuery()).getOrElse(matchAllQuery)
      } sourceInclude fields

      _search = excludes match {
        case Nil      => _search
        case excludes => _search sourceExclude excludes
      }

      _search = aggregations match {
        case Nil => _search
        case _   => _search aggregations { aggregations.map(_.agg) }
      }

      _search = orderBy match {
        case Some(o) =>
          _search sortBy o.sorts.map(sort =>
            sort.order match {
              case Some(DESC) => FieldSort(sort.field).desc()
              case _          => FieldSort(sort.field).asc()
            }
          )
        case _ => _search
      }

      if (aggregations.nonEmpty && fields.isEmpty) {
        _search size 0
      } else {
        limit match {
          case Some(l) => _search limit l.limit from 0
          case _       => _search
        }
      }
    }
  }

  case class SQLQuery(query: String) {
    import SQLImplicits._

    def select: Option[Either[ElasticSearchRequest, ElasticMultiSearchRequest]] = {
      val select: Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = query
      select map {
        case Left(s) => Left(s)
        case Right(m) =>
          Right(ElasticMultiSearchRequest(m.selects.map(_.asInstanceOf), m.multiSearchRequest))
      }
    }

    def aggregations: Seq[ElasticAggregation] = {
//      search.map(s => s.aggregates.map(_.copy(sources = s.sources, query = Some(s.query)))).getOrElse(Seq.empty)
      import com.sksamuel.elastic4s.ElasticApi._
      val select: Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = this.query
      select
        .map { case Left(l) =>
          l.aggregations.map(aggregation => {

            val queryFiltered = l.where.map(_.asQuery()).getOrElse(matchAllQuery)

            aggregation.copy(
              sources = l.sources,
              query = Some(
                (aggregation.aggType match {
                  case Count if aggregation.sourceField.equalsIgnoreCase("_id") =>
                    SearchBodyBuilderFn(
                      ElasticApi.search("") query {
                        queryFiltered
                      }
                    )
                  case _ =>
                    SearchBodyBuilderFn(
                      ElasticApi.search("") query {
                        queryFiltered
                      }
                      aggregations {
                        aggregation.agg
                      }
                      size 0
                    )
                }).string().replace("\"version\":true,", "") /*FIXME*/
              )
            )
          })

        }
        .getOrElse(Seq.empty)
    }

    def search: Option[ElasticSearchRequest] = select match {
      case Some(Left(value)) => Some(value)
      case _                 => None
    }

    def multiSearch: Option[ElasticMultiSearchRequest] = select match {
      case Some(Right(value)) => Some(value)
      case _                  => None
    }
  }

}
