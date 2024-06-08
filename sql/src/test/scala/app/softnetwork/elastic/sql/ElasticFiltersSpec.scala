package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.SearchRequest
import com.sksamuel.elastic4s.searches.queries.Query
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Created by smanciot on 13/04/17.
  */
class ElasticFiltersSpec extends AnyFlatSpec with Matchers {

  import Queries._

  import scala.language.implicitConversions

  def query2String(result: Query): String = {
    SearchBodyBuilderFn(SearchRequest("*") query result).string()
  }

  "ElasticFilters" should "filter numerical eq" in {
    val result = ElasticFilters.filter(numericalEq)
    query2String(result) shouldBe """{

        |"query":{
        |    "term" : {
        |      "identifier" : {
        |        "value" : "1.0"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical ne" in {
    val result = ElasticFilters.filter(numericalNe)
    query2String(result) shouldBe """{

        |"query":{
        |   "bool":{
        |       "must_not":[
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
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical lt" in {
    val result = ElasticFilters.filter(numericalLt)
    query2String(result) shouldBe """{

        |"query":{
        |    "range" : {
        |      "identifier" : {
        |        "lt" : "1"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical le" in {
    val result = ElasticFilters.filter(numericalLe)
    query2String(result) shouldBe """{

        |"query":{
        |    "range" : {
        |      "identifier" : {
        |        "lte" : "1"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical gt" in {
    val result = ElasticFilters.filter(numericalGt)
    query2String(result) shouldBe """{

        |"query":{
        |    "range" : {
        |      "identifier" : {
        |        "gt" : "1"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical ge" in {
    val result = ElasticFilters.filter(numericalGe)
    query2String(result) shouldBe """{

        |"query":{
        |    "range" : {
        |      "identifier" : {
        |        "gte" : "1"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal eq" in {
    val result = ElasticFilters.filter(literalEq)
    query2String(result) shouldBe """{

        |"query":{
        |    "term" : {
        |      "identifier" : {
        |        "value" : "un"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal ne" in {
    val result = ElasticFilters.filter(literalNe)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
        |      "must_not" : [
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
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal like" in {
    val result = ElasticFilters.filter(literalLike)
    query2String(result) shouldBe """{

        |"query":{
        |    "regexp" : {
        |      "identifier" : {
        |        "value" : ".*?un.*?"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter between" in {
    val result = ElasticFilters.filter(betweenExpression)
    query2String(result) shouldBe """{

        |"query":{
        |    "range" : {
        |      "identifier" : {
        |        "gte" : "1",
        |        "lte" : "2"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter and predicate" in {
    val result = ElasticFilters.filter(andPredicate)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(orPredicate)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(leftPredicate)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(rightPredicate)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(predicates)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(inLiteralExpression)
    query2String(result) shouldBe """{

        |"query":{
        |    "terms" : {
        |      "identifier" : [
        |        "val1",
        |        "val2",
        |        "val3"
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter in numerical expression with Int values" in {
    val result = ElasticFilters.filter(inNumericalExpressionWithIntValues)
    query2String(result) shouldBe """{

        |"query":{
        |    "terms" : {
        |      "identifier" : [
        |        1,
        |        2,
        |        3
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter in numerical expression with Double values" in {
    val result = ElasticFilters.filter(inNumericalExpressionWithDoubleValues)
    query2String(result) shouldBe """{

        |"query":{
        |    "terms" : {
        |      "identifier" : [
        |        1.0,
        |        2.1,
        |        3.4
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter nested predicate" in {
    val result = ElasticFilters.filter(nestedPredicate)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(nestedCriteria)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(childPredicate)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(childCriteria)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(parentPredicate)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(parentCriteria)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
    val result = ElasticFilters.filter(nestedWithBetween)
    query2String(result) shouldBe """{

        |"query":{
        |    "nested" : {
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
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter boolean eq" in {
    val result = ElasticFilters.filter(boolEq)
    query2String(result) shouldBe """{

        |"query":{
        |    "term" : {
        |      "identifier" : {
        |        "value" : true
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter boolean ne" in {
    val result = ElasticFilters.filter(boolNe)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
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
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter is null" in {
    val result = ElasticFilters.filter(isNull)
    query2String(result) shouldBe """{

        |"query":{
        |    "bool" : {
        |      "must_not" : [
        |        {
        |          "exists" : {
        |            "field" : "identifier"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter is not null" in {
    val result = ElasticFilters.filter(isNotNull)
    query2String(result) shouldBe """{

        |"query":{
        |    "exists" : {
        |      "field" : "identifier"
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter geo distance criteria" in {
    val result = ElasticFilters.filter(geoDistanceCriteria)
    query2String(result) shouldBe
    """{

        |"query":{
        |    "geo_distance" : {
        |      "distance":"5km",
        |      "profile.location":[40.0,-70.0]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

}
