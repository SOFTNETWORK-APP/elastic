package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi.{
  avgAgg,
  cardinalityAgg,
  filterAgg,
  matchAllQuery,
  maxAgg,
  minAgg,
  nestedAggregation,
  sumAgg,
  valueCountAgg
}
import com.sksamuel.elastic4s.searches.aggs.Aggregation

case object SELECT extends SQLExpr("select")

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

case class SQLExcept(fields: Seq[SQLField]) extends Updateable {
  override def sql: String = s" except(${fields.map(_.sql).mkString(",")})"
  def update(request: SQLSearchRequest): SQLExcept =
    this.copy(fields = fields.map(_.update(request)))
}

case object FILTER extends SQLExpr("filter")

case class SQLFilter(criteria: Option[SQLCriteria]) extends Updateable {
  override def sql: String = criteria match {
    case Some(c) => s" $FILTER($c)"
    case _       => ""
  }
  def update(request: SQLSearchRequest): SQLFilter =
    this.copy(criteria = criteria.map(_.update(request)))
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
