package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn

case class SQLQuery(query: String, score: Option[Double] = None) {
  import SQLImplicits._

  lazy val select: Option[Either[ElasticSearchRequest, ElasticMultiSearchRequest]] = {
    val select: Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = query
    select map {
      case Left(s) => Left(s)
      case Right(m) =>
        Right(ElasticMultiSearchRequest(m.requests.map(_.asInstanceOf), m.multiSearchRequest))
    }
  }

  lazy val aggregations: Seq[ElasticAggregation] = {
    import com.sksamuel.elastic4s.ElasticApi._
    val select: Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = this.query
    select
      .map {
        case Left(l) =>
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

        case _ => Seq.empty

      }
      .getOrElse(Seq.empty)
  }

  lazy val search: Option[ElasticSearchRequest] = select match {
    case Some(Left(value)) => Some(value.minScore(score))
    case _                 => None
  }

  lazy val multiSearch: Option[ElasticMultiSearchRequest] = select match {
    case Some(Right(value)) => Some(value)
    case _                  => None
  }

  def minScore(score: Double): SQLQuery = this.copy(score = Some(score))
}
