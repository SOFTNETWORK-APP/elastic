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

  def filter(criteria: Option[SQLCriteria]): Query = {

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

    def _filter(criteria: SQLCriteria): Query = {
      criteria match {
        case ElasticGeoDistance(identifier, distance, lat, lon) =>
          geoDistanceQuery(identifier.identifier)
            .point(lat.value, lon.value) distance distance.value
        case SQLExpression(identifier, operator, value) =>
          value match {
            case n: SQLNumeric[Any] @unchecked =>
              operator match {
                case _: GE.type => rangeQuery(identifier.identifier) gte n.sql
                case _: GT.type => rangeQuery(identifier.identifier) gt n.sql
                case _: LE.type => rangeQuery(identifier.identifier) lte n.sql
                case _: LT.type => rangeQuery(identifier.identifier) lt n.sql
                case _: EQ.type => termQuery(identifier.identifier, n.sql)
                case _: NE.type => not(termQuery(identifier.identifier, n.sql))
                case _          => matchAllQuery
              }
            case l: SQLLiteral =>
              operator match {
                case _: LIKE.type => regexQuery(identifier.identifier, toRegex(l.value))
                case _: GE.type   => rangeQuery(identifier.identifier) gte l.value
                case _: GT.type   => rangeQuery(identifier.identifier) gt l.value
                case _: LE.type   => rangeQuery(identifier.identifier) lte l.value
                case _: LT.type   => rangeQuery(identifier.identifier) lt l.value
                case _: EQ.type   => termQuery(identifier.identifier, l.value)
                case _: NE.type   => not(termQuery(identifier.identifier, l.value))
                case _            => matchAllQuery
              }
            case b: SQLBoolean =>
              operator match {
                case _: EQ.type => termQuery(identifier.identifier, b.value)
                case _: NE.type => not(termQuery(identifier.identifier, b.value))
                case _          => matchAllQuery
              }
            case _ => matchAllQuery
          }
        case SQLIsNull(identifier)    => not(existsQuery(identifier.identifier))
        case SQLIsNotNull(identifier) => existsQuery(identifier.identifier)
        case SQLPredicate(left, operator, right, _not) =>
          operator match {
            case _: AND.type =>
              if (_not.isDefined)
                bool(Seq(_filter(left)), Seq.empty, Seq(_filter(right)))
              else
                boolQuery().filter(_filter(left), _filter(right))
            case _: OR.type => should(_filter(left), _filter(right))
            case _          => matchAllQuery
          }
        case SQLIn(identifier, values, n) =>
          val _values: Seq[Any] = values.innerValues
          val t =
            _values.headOption match {
              case Some(_: Double) =>
                termsQuery(identifier.identifier, _values.asInstanceOf[Seq[Double]])
              case Some(_: Integer) =>
                termsQuery(identifier.identifier, _values.asInstanceOf[Seq[Integer]])
              case Some(_: Long) =>
                termsQuery(identifier.identifier, _values.asInstanceOf[Seq[Long]])
              case _ => termsQuery(identifier.identifier, _values.map(_.toString))
            }
          n match {
            case Some(_) => not(t)
            case None    => t
          }
        case SQLBetween(identifier, from, to) =>
          rangeQuery(identifier.identifier) gte from.value lte to.value
        case relation: ElasticRelation =>
          import scala.language.reflectiveCalls
          val t = relation.`type`
          t match {
            case Some(_) =>
              relation match {
                case _: ElasticNested =>
                  nestedQuery(t.get, _filter(relation.criteria)).inner(innerHits(_innerHit(t.get)))
                case _: ElasticChild =>
                  hasChildQuery(t.get, _filter(relation.criteria), ScoreMode.None)
                case _: ElasticParent =>
                  hasParentQuery(t.get, _filter(relation.criteria), score = false)
                case _ => matchAllQuery
              }
            case _ => matchAllQuery
          }
        case _ => matchAllQuery
      }
    }

    criteria match {
      case Some(c) => _filter(c)
      case _       => matchAllQuery
    }

  }

}
