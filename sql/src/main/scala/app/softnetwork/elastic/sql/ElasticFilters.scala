package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.searches.ScoreMode
import com.sksamuel.elastic4s.searches.queries.Query
import com.sksamuel.elastic4s.searches.queries.term.{BuildableTermsQuery, TermsQuery}

import scala.annotation.tailrec

/** Created by smanciot on 27/06/2018.
  */
object ElasticFilters {

  import SQLImplicits._

  implicit def BuildableTermsNoOp[T]: BuildableTermsQuery[T] = new BuildableTermsQuery[T] {
    override def build(q: TermsQuery[T]): Any = null // not used by the http builders
  }

  def filter(query: String): Query = {
    val criteria: Option[SQLCriteria] = query
    filter(criteria)
  }

  def filter(criteria: Option[SQLCriteria], insideNestedAgg: Option[String] = None): Query = {

    var _innerHits: Set[String] = Set.empty

    @tailrec
    def _innerHit(name: String, inc: Int = 1): String = {
      if (_innerHits.contains(name)) {
        val incName = s"$name$inc"
        if (_innerHits.contains(incName)) {
          _innerHit(name, inc + 1)
        } else {
          _innerHits += incName
          incName
        }
      } else {
        _innerHits += name
        name
      }
    }

    def nest(
      nestedType: Option[String],
      query: Query,
      insideNested: Option[String] = None
    ): Query = {
      nestedType match {
        case Some(n) if !insideNested.contains(n) =>
          nestedQuery(n).query(query).inner(innerHits(_innerHit(n)))
        case _ => query
      }
    }

    def _filter(criteria: SQLCriteria, insideNested: Option[String] = None): Query = {
      criteria match {
        case ElasticGeoDistance(identifier, distance, lat, lon) =>
          nest(
            identifier.nestedType,
            geoDistanceQuery(identifier.columnName)
              .point(lat.value, lon.value) distance distance.value
          )
        case SQLExpression(identifier, operator, value, _not_) =>
          value match {
            case n: SQLNumeric[Any] @unchecked =>
              operator match {
                case _: GE.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) lt n.sql,
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) gte n.sql,
                        insideNested
                      )
                  }
                case _: GT.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) lte n.sql,
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) gt n.sql,
                        insideNested
                      )
                  }
                case _: LE.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) gt n.sql,
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) lte n.sql,
                        insideNested
                      )
                  }
                case _: LT.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) gte n.sql,
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) lt n.sql,
                        insideNested
                      )
                  }
                case _: EQ.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        not(termQuery(identifier.columnName, n.sql)),
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        termQuery(identifier.columnName, n.sql),
                        insideNested
                      )
                  }
                case _: NE.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        termQuery(identifier.columnName, n.sql),
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        not(termQuery(identifier.columnName, n.sql)),
                        insideNested
                      )
                  }
                case _ => matchAllQuery
              }
            case l: SQLLiteral =>
              operator match {
                case _: LIKE.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        not(regexQuery(identifier.columnName, toRegex(l.value))),
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        regexQuery(identifier.columnName, toRegex(l.value)),
                        insideNested
                      )
                  }
                case _: GE.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) lt l.value,
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) gte l.value,
                        insideNested
                      )
                  }
                case _: GT.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) lte l.value,
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) gt l.value,
                        insideNested
                      )
                  }
                case _: LE.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) gt l.value,
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) lte l.value,
                        insideNested
                      )
                  }
                case _: LT.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) gte l.value,
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        rangeQuery(identifier.columnName) lt l.value,
                        insideNested
                      )
                  }
                case _: EQ.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        not(termQuery(identifier.columnName, l.value)),
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        termQuery(identifier.columnName, l.value),
                        insideNested
                      )
                  }
                case _: NE.type =>
                  _not_ match {
                    case Some(_) =>
                      nest(
                        identifier.nestedType,
                        termQuery(identifier.columnName, l.value),
                        insideNested
                      )
                    case _ =>
                      nest(
                        identifier.nestedType,
                        not(termQuery(identifier.columnName, l.value)),
                        insideNested
                      )
                  }
                case _ => matchAllQuery
              }
            case b: SQLBoolean =>
              operator match {
                case _: EQ.type =>
                  nest(
                    identifier.nestedType,
                    termQuery(identifier.columnName, b.value),
                    insideNested
                  )
                case _: NE.type =>
                  nest(
                    identifier.nestedType,
                    not(termQuery(identifier.columnName, b.value)),
                    insideNested
                  )
                case _ => matchAllQuery
              }
            case _ => matchAllQuery
          }
        case SQLIsNull(identifier) =>
          nest(identifier.nestedType, not(existsQuery(identifier.columnName)), insideNested)
        case SQLIsNotNull(identifier) =>
          nest(identifier.nestedType, existsQuery(identifier.columnName), insideNested)
        case SQLPredicate(left, operator, right, _not_) =>
          operator match {
            case _: AND.type =>
              _not_ match {
                case Some(_) =>
                  bool(
                    Seq(_filter(left, insideNested)),
                    Seq.empty,
                    Seq(_filter(right, insideNested))
                  )
                case _ =>
                  boolQuery().filter(_filter(left, insideNested), _filter(right, insideNested))
              }
            case _: OR.type => should(_filter(left, insideNested), _filter(right, insideNested))
            case _          => matchAllQuery
          }
        case SQLIn(identifier, values, _not_) =>
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
          _not_ match {
            case Some(_) => nest(identifier.nestedType, not(t), insideNested)
            case None    => nest(identifier.nestedType, t, insideNested)
          }
        case SQLBetween(identifier, from, to) =>
          nest(
            identifier.nestedType,
            rangeQuery(identifier.columnName) gte from.value lte to.value,
            insideNested
          )
        case relation: ElasticRelation =>
          import scala.language.reflectiveCalls
          relation.`type` match {
            case Some(t) =>
              relation match {
                case _: ElasticNested =>
                  nestedQuery(t, _filter(relation.criteria, Some(t))).inner(innerHits(_innerHit(t)))
                case _: ElasticChild =>
                  hasChildQuery(t, _filter(relation.criteria, Some(t)), ScoreMode.None)
                case _: ElasticParent =>
                  hasParentQuery(t, _filter(relation.criteria, Some(t)), score = false)
                case _ => matchAllQuery
              }
            case _ => matchAllQuery
          }
        case _ => matchAllQuery
      }
    }

    criteria match {
      case Some(c) => _filter(c, insideNestedAgg)
      case _       => matchAllQuery
    }

  }

}
