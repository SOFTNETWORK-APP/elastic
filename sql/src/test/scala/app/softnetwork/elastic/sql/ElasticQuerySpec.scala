package app.softnetwork.elastic.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Created by smanciot on 13/04/17.
  */
class ElasticQuerySpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  "ElasticQuery" should "perform native count" in {
    val results = ElasticQuery.count(
      SQLQuery("select count($t.id) as c2 from Table as t where $t.nom = \"Nom\"")
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe false
    result.distinct shouldBe false
    result.agg shouldBe "agg_id"
    result.field shouldBe "c2"
    result.sources shouldBe Seq[String]("Table")
    result.query shouldBe
    """|{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "agg_id": {
        |      "value_count": {
        |        "field": "id"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform count distinct" in {
    val results = ElasticQuery.count(
      SQLQuery("select count(distinct $t.id) as c2 from Table as t where $t.nom = \"Nom\"")
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe false
    result.distinct shouldBe true
    result.agg shouldBe "agg_distinct_id"
    result.field shouldBe "c2"
    result.sources shouldBe Seq[String]("Table")
    result.query shouldBe
    """|{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "agg_distinct_id": {
        |      "cardinality": {
        |        "field": "id"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count" in {
    val results = ElasticQuery.count(
      SQLQuery("select count(email.value) as email from index where nom = \"Nom\"")
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.agg shouldBe "nested_email.agg_email_value"
    result.field shouldBe "email"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_email": {
        |      "nested": {
        |        "path": "email"
        |      },
        |      "aggs": {
        |        "agg_email_value": {
        |          "value_count": {
        |            "field": "email.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with nested criteria" in {
    val results = ElasticQuery.count(
      SQLQuery(
        "select count(email.value) as email from index where nom = \"Nom\" and (profile.postalCode in (\"75001\",\"75002\"))"
      )
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.agg shouldBe "nested_email.agg_email_value"
    result.field shouldBe "email"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |          {
        |            "term": {
        |              "nom": {
        |                "value": "Nom"
        |              }
        |            }
        |          },
        |          {
        |            "nested": {
        |              "path": "profile",
        |              "query": {
        |                "terms": {
        |                  "profile.postalCode": [
        |                    "75001",
        |                    "75002"
        |                  ]
        |                }
        |              },
        |              "inner_hits":{"name":"profile"}
        |            }
        |          }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_email": {
        |      "nested": {
        |        "path": "email"
        |      },
        |      "aggs": {
        |        "agg_email_value": {
        |          "value_count": {
        |            "field": "email.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with filter" in {
    val results = ElasticQuery.count(
      SQLQuery(
        "select count(email.value) as email filter[email.context = \"profile\"] from index where nom = \"Nom\" and (profile.postalCode in (\"75001\",\"75002\"))"
      )
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.agg shouldBe "nested_email.filtered_agg.agg_email_value"
    result.field shouldBe "email"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        },
        |        {
        |          "nested": {
        |            "path": "profile",
        |            "query": {
        |              "terms": {
        |                "profile.postalCode": [
        |                  "75001",
        |                  "75002"
        |                ]
        |              }
        |            },
        |            "inner_hits":{"name":"profile"}
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_email": {
        |      "nested": {
        |        "path": "email"
        |      },
        |      "aggs": {
        |        "filtered_agg": {
        |          "filter": {
        |            "term": {
        |              "email.context": {
        |                "value": "profile"
        |              }
        |            }
        |          },
        |          "aggs": {
        |            "agg_email_value": {
        |              "value_count": {
        |                "field": "email.value"
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "accept and not operator" in {
    val results = ElasticQuery.count(
      SQLQuery(
        "select count(distinct email.value) as email from index where (profile.postalCode = \"33600\" and not profile.postalCode = \"75001\")"
      )
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe true
    result.agg shouldBe "nested_email.agg_distinct_email_value"
    result.field shouldBe "email"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "must": [
        |        {
        |          "nested": {
        |            "path": "profile",
        |            "query": {
        |              "term": {
        |                "profile.postalCode": {
        |                  "value": "33600"
        |                }
        |              }
        |            },
        |            "inner_hits":{"name":"profile"}
        |          }
        |        }
        |      ],
        |      "must_not": [
        |        {
        |          "nested": {
        |            "path": "profile",
        |            "query": {
        |              "term": {
        |                "profile.postalCode": {
        |                  "value": "75001"
        |                }
        |              }
        |            },
        |            "inner_hits":{"name":"profile1"}
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_email": {
        |      "nested": {
        |        "path": "email"
        |      },
        |      "aggs": {
        |        "agg_distinct_email_value": {
        |          "cardinality": {
        |            "field": "email.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}
        |""".stripMargin.replaceAll("\\s+", "")
  }

  it should "accept date filtering" in {
    val results = ElasticQuery.count(
      SQLQuery(
        "select count(distinct email.value) as email from index where profile.postalCode = \"33600\" and profile.createdDate <= \"now-35M/M\""
      )
    )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe true
    result.agg shouldBe "nested_email.agg_distinct_email_value"
    result.field shouldBe "email"
    result.sources shouldBe Seq[String]("index")
    result.query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "nested": {
        |            "path": "profile",
        |            "query": {
        |              "term": {
        |                "profile.postalCode": {
        |                  "value": "33600"
        |                }
        |              }
        |            },
        |            "inner_hits":{"name":"profile"}
        |          }
        |        },
        |        {
        |          "nested": {
        |            "path": "profile",
        |            "query": {
        |              "range": {
        |                "profile.createdDate": {
        |                  "lte": "now-35M/M"
        |                }
        |              }
        |            },
        |            "inner_hits":{"name":"profile1"}
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_email": {
        |      "nested": {
        |        "path": "email"
        |      },
        |      "aggs": {
        |        "agg_distinct_email_value": {
        |          "cardinality": {
        |            "field": "email.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}
        |""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform select" in {
    val select = ElasticQuery.select(
      SQLQuery("""
        |SELECT
        |profileId,
        |profile_ccm.email as email,
        |profile_ccm.city as city,
        |profile_ccm.firstName as firstName,
        |profile_ccm.lastName as lastName,
        |profile_ccm.postalCode as postalCode,
        |profile_ccm.birthYear as birthYear
        |FROM index
        |WHERE
        |profile_ccm.postalCode BETWEEN "10" AND "99999"
        |AND
        |profile_ccm.birthYear <= 2000
        |limit 100""".stripMargin)
    )
    select.isDefined shouldBe true
    val result = select.get
    result.query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "nested": {
        |            "path": "profile_ccm",
        |            "query": {
        |              "range": {
        |                "profile_ccm.postalCode": {
        |                  "gte": "10",
        |                  "lte": "99999"
        |                }
        |              }
        |            },
        |            "inner_hits":{"name":"profile_ccm"}
        |          }
        |        },
        |        {
        |          "nested": {
        |            "path": "profile_ccm",
        |            "query": {
        |              "range": {
        |                "profile_ccm.birthYear": {
        |                  "lte": "2000"
        |                }
        |              }
        |            },
        |            "inner_hits":{"name":"profile_ccm1"}
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "from":0,
        |  "size":100,
        |  "_source": {
        |    "includes": [
        |      "profileId",
        |      "profile_ccm.email",
        |      "profile_ccm.city",
        |      "profile_ccm.firstName",
        |      "profile_ccm.lastName",
        |      "profile_ccm.postalCode",
        |      "profile_ccm.birthYear"
        |    ]
        |  }
        |}
        |""".stripMargin.replaceAll("\\s+", "")
  }

}
