package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi.matchAllQuery
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.SearchRequest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Created by smanciot on 13/04/17.
  */
class SQLCriteriaSpec extends AnyFlatSpec with Matchers {

  import Queries._

  import scala.language.implicitConversions

  def filter(sql: String): String = {
    import SQLImplicits._
    val criteria: Option[SQLCriteria] = sql
    val result = SearchBodyBuilderFn(
      SearchRequest("*") query criteria
        .map(_.filter(None).query(currentQuery = None))
        .getOrElse(matchAllQuery())
    ).string()
    println(result)
    result
  }

  "SQLCriteria" should "filter numerical eq" in {
    filter(numericalEq) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"term" : {
        |      "identifier" : {
        |        "value" : "1.0"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical ne" in {
    filter(numericalNe) shouldBe """{

        |"query":{
        |   "bool":{
        |       "filter":[{"bool":{"must_not":[
        |         {
        |           "term":{
        |             "identifier":{
        |               "value":"1"
        |             }
        |           }
        |         }
        |       ]
        |    }
        | }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical lt" in {
    filter(numericalLt) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "lt" : "1"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical le" in {
    filter(numericalLe) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "lte" : "1"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical gt" in {
    filter(numericalGt)
    filter(numericalGt) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "gt" : "1"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical ge" in {
    filter(numericalGe) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "gte" : "1"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal eq" in {
    filter(literalEq) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"term" : {
        |      "identifier" : {
        |        "value" : "un"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal ne" in {
    filter(literalNe) shouldBe """{

        |"query":{
        |    "bool" : {
        |      "filter":[{"bool":{"must_not" : [
        |        {
        |          "term" : {
        |            "identifier" : {
        |              "value" : "un"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal like" in {
    filter(literalLike) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"regexp" : {
        |      "identifier" : {
        |        "value" : ".*?un.*?"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal not like" in {
    filter(literalNotLike) shouldBe """{
        |"query":{
        |    "bool": {
        |      "filter":[{"bool":{"must_not": [{
        |        "regexp": {
        |          "identifier": {
        |            "value": ".*?un.*?"
        |          }
        |        }
        |      }]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter between" in {
    filter(betweenExpression) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "gte" : "1",
        |        "lte" : "2"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter and predicate" in {
    filter(andPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : "1"
        |            }
        |          }
        |        },
        |        {
        |          "range" : {
        |            "identifier2" : {
        |              "gt" : "2"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter or predicate" in {
    filter(orPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "should" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : "1"
        |            }
        |          }
        |        },
        |        {
        |          "range" : {
        |            "identifier2" : {
        |              "gt" : "2"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter left predicate with criteria" in {
    filter(leftPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "should" : [
        |        {
        |          "bool" : {
        |            "filter" : [
        |              {
        |                "term" : {
        |                  "identifier1" : {
        |                    "value" : "1"
        |                  }
        |                }
        |              },
        |              {
        |                "range" : {
        |                  "identifier2" : {
        |                    "gt" : "2"
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "term" : {
        |            "identifier3" : {
        |              "value" : "3"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter right predicate with criteria" in {
    filter(rightPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : "1"
        |            }
        |          }
        |        },
        |        {
        |          "bool" : {
        |            "should" : [
        |              {
        |                "range" : {
        |                  "identifier2" : {
        |                    "gt" : "2"
        |                  }
        |                }
        |              },
        |              {
        |                "term" : {
        |                  "identifier3" : {
        |                    "value" : "3"
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter multiple predicates" in {
    filter(predicates) shouldBe """{

        |"query":{
        |    "bool":{
        |      "should" : [
        |        {
        |          "bool" : {
        |            "filter" : [
        |              {
        |                "term" : {
        |                  "identifier1" : {
        |                    "value" : "1"
        |                  }
        |                }
        |              },
        |              {
        |                "range" : {
        |                  "identifier2" : {
        |                    "gt" : "2"
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "bool" : {
        |            "filter" : [
        |              {
        |                "term" : {
        |                  "identifier3" : {
        |                    "value" : "3"
        |                  }
        |                }
        |              },
        |              {
        |                "term" : {
        |                  "identifier4" : {
        |                    "value" : "4"
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter in literal expression" in {
    filter(inLiteralExpression) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"terms" : {
        |      "identifier" : [
        |        "val1",
        |        "val2",
        |        "val3"
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter in numerical expression with Int values" in {
    filter(inNumericalExpressionWithIntValues) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"terms" : {
        |      "identifier" : [
        |        1,
        |        2,
        |        3
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter in numerical expression with Double values" in {
    filter(inNumericalExpressionWithDoubleValues) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"terms" : {
        |      "identifier" : [
        |        1.0,
        |        2.1,
        |        3.4
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter nested predicate" in {
    filter(nestedPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : "1"
        |            }
        |          }
        |        },
        |        {
        |          "nested" : {
        |            "path" : "nested",
        |            "query" : {
        |              "bool" : {
        |                "should" : [
        |                  {
        |                    "range" : {
        |                      "nested.identifier2" : {
        |                        "gt" : "2"
        |                      }
        |                    }
        |                  },
        |                  {
        |                    "term" : {
        |                      "nested.identifier3" : {
        |                        "value" : "3"
        |                      }
        |                    }
        |                  }
        |                ]
        |              }
        |            },
        |            "inner_hits":{"name":"nested"}
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter nested criteria" in {
    filter(nestedCriteria) shouldBe """{

        |"query":{
        |    "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : "1"
        |            }
        |          }
        |        },
        |        {
        |          "nested" : {
        |            "path" : "nested",
        |            "query" : {
        |              "term" : {
        |                "nested.identifier3" : {
        |                  "value" : "3"
        |                }
        |              }
        |            },
        |            "inner_hits":{"name":"nested"}
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter child predicate" in {
    filter(childPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : "1"
        |            }
        |          }
        |        },
        |        {
        |          "has_child" : {
        |            "type" : "child",
        |            "score_mode" : "none",
        |            "query" : {
        |              "bool" : {
        |                "should" : [
        |                  {
        |                    "range" : {
        |                      "child.identifier2" : {
        |                        "gt" : "2"
        |                      }
        |                    }
        |                  },
        |                  {
        |                    "term" : {
        |                      "child.identifier3" : {
        |                        "value" : "3"
        |                      }
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter child criteria" in {
    filter(childCriteria) shouldBe """{

        |"query":{
        |   "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : "1"
        |            }
        |          }
        |        },
        |        {
        |          "has_child" : {
        |            "type" : "child",
        |            "score_mode" : "none",
        |            "query" : {
        |              "term" : {
        |                "child.identifier3" : {
        |                  "value" : "3"
        |                }
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter parent predicate" in {
    filter(parentPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : "1"
        |            }
        |          }
        |        },
        |        {
        |          "has_parent" : {
        |            "parent_type" : "parent",
        |            "query" : {
        |              "bool" : {
        |                "should" : [
        |                  {
        |                    "range" : {
        |                      "parent.identifier2" : {
        |                        "gt" : "2"
        |                      }
        |                    }
        |                  },
        |                  {
        |                    "term" : {
        |                      "parent.identifier3" : {
        |                        "value" : "3"
        |                      }
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter parent criteria" in {
    filter(parentCriteria) shouldBe """{

        |"query":{
        |    "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : "1"
        |            }
        |          }
        |        },
        |        {
        |          "has_parent" : {
        |            "parent_type" : "parent",
        |            "query" : {
        |              "term" : {
        |                "parent.identifier3" : {
        |                  "value" : "3"
        |                }
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter nested with between" in {
    filter(nestedWithBetween) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"nested" : {
        |      "path" : "ciblage",
        |      "query" : {
        |        "bool" : {
        |          "filter" : [
        |            {
        |              "range" : {
        |                "ciblage.Archivage_CreationDate" : {
        |                  "gte" : "now-3M/M",
        |                  "lte" : "now"
        |                }
        |              }
        |            },
        |            {
        |              "term" : {
        |                "ciblage.statutComportement" : {
        |                  "value" : "1"
        |                }
        |              }
        |            }
        |          ]
        |        }
        |      },
        |      "inner_hits":{"name":"ciblage"}
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter boolean eq" in {
    filter(boolEq) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"term" : {
        |      "identifier" : {
        |        "value" : true
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter boolean ne" in {
    filter(boolNe) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"bool" : {
        |      "must_not" : [
        |        {
        |          "term" : {
        |            "identifier" : {
        |              "value" : false
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter is null" in {
    filter(isNull) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"bool" : {
        |      "must_not" : [
        |        {
        |          "exists" : {
        |            "field" : "identifier"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter is not null" in {
    filter(isNotNull) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"exists" : {
        |      "field" : "identifier"
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter geo distance criteria" in {
    filter(geoDistanceCriteria) shouldBe
    """{

        |"query": {
        |    "bool":{"filter":[{"geo_distance": {
        |      "distance": "5km",
        |      "profile.location": [
        |        40.0,
        |        -70.0
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter match criteria" in {
    filter(matchCriteria) shouldBe
    """{
        | "query":{
        |   "bool":{
        |     "filter":[
        |       {
        |         "match":{
        |           "identifier":{
        |             "query":"value"
        |           }
        |         }
        |       }
        |     ]
        |   }
        | }
        | }""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter complex queries" in {
    val query =
      """select * from Table
        |where (identifier is not null and identifier = 1) or
        |(
        | (identifier is null or identifier2 > 2)
        | and identifier3 = 3
        |)""".stripMargin
    filter(query) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "should": [
        |        {
        |          "bool": {
        |            "filter": [
        |              {
        |                "exists": {
        |                  "field": "identifier"
        |                }
        |              },
        |              {
        |                "term": {
        |                  "identifier": {
        |                    "value": "1"
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "bool": {
        |            "filter": [
        |              {
        |                "bool": {
        |                  "should": [
        |                    {
        |                      "bool": {
        |                        "must_not": [
        |                          {
        |                            "exists": {
        |                              "field": "identifier"
        |                            }
        |                          }
        |                        ]
        |                      }
        |                    },
        |                    {
        |                      "range": {
        |                        "identifier2": {
        |                          "gt": "2"
        |                        }
        |                      }
        |                    }
        |                  ]
        |                }
        |              },
        |              {
        |                "term": {
        |                  "identifier3": {
        |                    "value": "3"
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}
        |""".stripMargin.replaceAll("\\s", "")
  }

}
