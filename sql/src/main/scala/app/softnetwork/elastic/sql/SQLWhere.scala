package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.requests.searches.queries.Query

case object Where extends SQLExpr("where") with SQLRegex

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

case class SQLPredicate(
  leftCriteria: SQLCriteria,
  operator: SQLPredicateOperator,
  rightCriteria: SQLCriteria,
  not: Option[Not.type] = None,
  group: Boolean = false
) extends SQLCriteria {
  override def sql = s"${if (group) s"($leftCriteria"
  else leftCriteria} $operator${not
    .map(_ => " not")
    .getOrElse("")} ${if (group) s"$rightCriteria)" else rightCriteria}"
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
      case And =>
        (not match {
          case Some(_) => query.not(rightCriteria.asFilter(Option(query)))
          case _       => query.filter(rightCriteria.asFilter(Option(query)))
        }).filter(leftCriteria.asFilter(Option(query)))
      case Or =>
        (not match {
          case Some(_) => query.not(rightCriteria.asFilter(Option(query)))
          case _       => query.should(rightCriteria.asFilter(Option(query)))
        }).should(leftCriteria.asFilter(Option(query)))
    }
  }

  override def nested: Boolean = leftCriteria.nested && rightCriteria.nested

  override def matchCriteria: Boolean = leftCriteria.matchCriteria || rightCriteria.matchCriteria
}

sealed trait ElasticFilter {
  def query(
    innerHitsNames: Set[String] = Set.empty,
    currentQuery: Option[ElasticBoolQuery]
  ): Query
}

sealed trait SQLCriteriaWithIdentifier extends SQLCriteria {
  def identifier: SQLIdentifier
  override def nested: Boolean = identifier.nested
  override def group: Boolean = false
  override lazy val limit: Option[SQLLimit] = identifier.limit
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

case class SQLExpression(
  identifier: SQLIdentifier,
  operator: SQLExpressionOperator,
  value: SQLToken,
  maybeNot: Option[Not.type] = None
) extends SQLCriteriaWithIdentifier
    with ElasticFilter {
  override def sql =
    s"$identifier ${maybeNot.map(_ => "not ").getOrElse("")}$operator $value"
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
          case _: Ge.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) lt n.sql
              case _ =>
                rangeQuery(identifier.columnName) gte n.sql
            }
          case _: Gt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) lte n.sql
              case _ =>
                rangeQuery(identifier.columnName) gt n.sql
            }
          case _: Le.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) gt n.sql
              case _ =>
                rangeQuery(identifier.columnName) lte n.sql
            }
          case _: Lt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) gte n.sql
              case _ =>
                rangeQuery(identifier.columnName) lt n.sql
            }
          case _: Eq.type =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.columnName, n.sql))
              case _ =>
                termQuery(identifier.columnName, n.sql)
            }
          case _: Ne.type =>
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
          case _: Like.type =>
            maybeNot match {
              case Some(_) =>
                not(regexQuery(identifier.columnName, toRegex(l.value)))
              case _ =>
                regexQuery(identifier.columnName, toRegex(l.value))
            }
          case _: Ge.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) lt l.value
              case _ =>
                rangeQuery(identifier.columnName) gte l.value
            }
          case _: Gt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) lte l.value
              case _ =>
                rangeQuery(identifier.columnName) gt l.value
            }
          case _: Le.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) gt l.value
              case _ =>
                rangeQuery(identifier.columnName) lte l.value
            }
          case _: Lt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) gte l.value
              case _ =>
                rangeQuery(identifier.columnName) lt l.value
            }
          case _: Eq.type =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.columnName, l.value))
              case _ =>
                termQuery(identifier.columnName, l.value)
            }
          case _: Ne.type =>
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
          case _: Eq.type =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.columnName, b.value))
              case _ =>
                termQuery(identifier.columnName, b.value)
            }
          case _: Ne.type =>
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
  override val operator: SQLOperator = IsNull
  override def sql = s"$identifier $operator"
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
  override val operator: SQLOperator = IsNotNull
  override def sql = s"$identifier $operator"
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
  maybeNot: Option[Not.type] = None
) extends SQLCriteriaWithIdentifier
    with ElasticFilter { this: SQLIn[R, T] =>
  override def sql =
    s"$identifier ${maybeNot.map(_ => "not ").getOrElse("")}$operator $values"
  override def operator: SQLOperator = In
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

case class SQLBetween(
  identifier: SQLIdentifier,
  from: SQLLiteral,
  to: SQLLiteral,
  maybeNot: Option[Not.type]
) extends SQLCriteriaWithIdentifier
    with ElasticFilter {
  override def sql =
    s"$identifier ${maybeNot.map(_ => "not ").getOrElse("")}$operator $from and $to"
  override def operator: SQLOperator = Between
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
    val r = rangeQuery(identifier.columnName) gte from.value lte to.value
    maybeNot match {
      case Some(_) => not(r)
      case _       => r
    }
  }
}

case class ElasticGeoDistance(
  identifier: SQLIdentifier,
  distance: SQLLiteral,
  lat: SQLDouble,
  lon: SQLDouble
) extends SQLCriteriaWithIdentifier
    with ElasticFilter {
  override def sql = s"$operator($identifier,($lat,$lon)) <= $distance"
  override def operator: SQLOperator = Distance
  override def update(request: SQLSearchRequest): ElasticGeoDistance =
    this.copy(identifier = identifier.update(request))

  override def asFilter(currentQuery: Option[ElasticBoolQuery]): ElasticFilter = this

  override def query(
    innerHitsNames: Set[String] = Set.empty,
    currentQuery: Option[ElasticBoolQuery]
  ): Query = {
    geoDistanceQuery(identifier.columnName, lat.value, lon.value) distance distance.value
  }
}

case class ElasticMatch(
  identifier: SQLIdentifier,
  value: SQLLiteral,
  options: Option[String]
) extends SQLCriteriaWithIdentifier
    with ElasticFilter {
  override def sql: String =
    s"$operator($identifier,$value${options.map(o => s""","$o"""").getOrElse("")})"
  override def operator: SQLOperator = Match
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

sealed abstract class ElasticRelation(val criteria: SQLCriteria, val operator: ElasticOperator)
    extends SQLCriteria
    with ElasticFilter {
  override def sql = s"$operator($criteria)"

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
    extends ElasticRelation(criteria, Nested) {
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
    extends ElasticRelation(criteria, Child) {
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
    extends ElasticRelation(criteria, Parent) {
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

case class SQLWhere(criteria: Option[SQLCriteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $Where $c"
    case _       => ""
  }
  def update(request: SQLSearchRequest): SQLWhere =
    this.copy(criteria = criteria.map(_.update(request)))

  def asQuery(group: Boolean = true, innerHitsNames: Set[String] = Set.empty): Query = criteria
    .map(_.asQuery(group = group, innerHitsNames = innerHitsNames))
    .getOrElse(matchAllQuery)
}
