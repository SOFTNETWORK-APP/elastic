package app.softnetwork.elastic.sql

import scala.util.parsing.combinator.RegexParsers

/** Created by smanciot on 27/06/2018.
  *
  * SQL Parser for ElasticSearch
  *
  * TODO add support for SQL :
  *   - EXCEPT,
  *   - UNION,
  *   - UNNEST,
  *   - JOIN,
  *   - GROUP BY,
  *   - ORDER BY,
  *   - HAVING, etc.
  */
object SQLParser extends RegexParsers {

  val regexAlias = """\b(?!(?i)where\b)\b(?!(?i)from\b)[a-zA-Z0-9_]*"""

  def identifier: Parser[SQLIdentifier] =
    "(?i)distinct".r.? ~ """[\*a-zA-Z_\-][a-zA-Z0-9_\-\.\[\]]*""".r ^^ { case d ~ str =>
      SQLIdentifier(
        str,
        None,
        d
      )
    }

  def literal: Parser[SQLLiteral] =
    """"[^"]*"|'[^']*'""".r ^^ (str => SQLLiteral(str.substring(1, str.length - 1)))

  def int: Parser[SQLInt] = """(-)?(0|[1-9]\d*)""".r ^^ (str => SQLInt(str.toInt))

  def double: Parser[SQLDouble] = """(-)?(\d+\.\d+)""".r ^^ (str => SQLDouble(str.toDouble))

  def boolean: Parser[SQLBoolean] = """(true|false)""".r ^^ (bool => SQLBoolean(bool.toBoolean))

  def eq: Parser[SQLExpressionOperator] = "=" ^^ (_ => EQ)
  def ge: Parser[SQLExpressionOperator] = ">=" ^^ (_ => GE)
  def gt: Parser[SQLExpressionOperator] = ">" ^^ (_ => GT)
  def in: Parser[SQLExpressionOperator] = "(?i)in".r ^^ (_ => IN)
  def le: Parser[SQLExpressionOperator] = "<=" ^^ (_ => LE)
  def like: Parser[SQLExpressionOperator] = "(?i)like".r ^^ (_ => LIKE)
  def lt: Parser[SQLExpressionOperator] = "<" ^^ (_ => LT)
  def ne: Parser[SQLExpressionOperator] = "<>" ^^ (_ => NE)

  def isNull: Parser[SQLExpressionOperator] = "(?i)(is null)".r ^^ (_ => IS_NULL)
  def isNullExpression: Parser[SQLCriteria] = identifier ~ isNull ^^ { case i ~ _ => SQLIsNull(i) }

  def isNotNull: Parser[SQLExpressionOperator] = "(?i)(is not null)".r ^^ (_ => IS_NOT_NULL)
  def isNotNullExpression: Parser[SQLCriteria] = identifier ~ isNotNull ^^ { case i ~ _ =>
    SQLIsNotNull(i)
  }

  def equalityExpression: Parser[SQLExpression] =
    not.? ~ identifier ~ (eq | ne) ~ (boolean | literal | double | int) ^^ { case n ~ i ~ o ~ v =>
      SQLExpression(i, o, v, n)
    }
  def likeExpression: Parser[SQLExpression] = identifier ~ not.? ~ like ~ literal ^^ {
    case i ~ n ~ o ~ v =>
      SQLExpression(i, o, v, n)
  }
  def comparisonExpression: Parser[SQLExpression] =
    not.? ~ identifier ~ (ge | gt | le | lt) ~ (double | int | literal) ^^ { case n ~ i ~ o ~ v =>
      SQLExpression(i, o, v, n)
    }

  def inLiteralExpression: Parser[SQLCriteria] =
    identifier ~ not.? ~ in ~ start ~ rep1(literal ~ separator.?) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ => SQLIn(i, SQLLiteralValues(v map { _._1 }), n)
    }
  def inNumericalExpression: Parser[SQLCriteria] =
    identifier ~ not.? ~ in ~ start ~ rep1((double | int) ~ separator.?) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ => SQLIn(i, SQLNumericValues(v map { _._1 }), n)
    }

  def between: Parser[SQLExpressionOperator] = "(?i)between".r ^^ (_ => BETWEEN)
  def betweenExpression: Parser[SQLCriteria] = identifier ~ between ~ literal ~ and ~ literal ^^ {
    case i ~ _ ~ from ~ _ ~ to => SQLBetween(i, from, to)
  }

  def distance: Parser[SQLFunction] = "(?i)distance".r ^^ (_ => SQLDistance)
  def distanceExpression: Parser[SQLCriteria] =
    distance ~ start ~ identifier ~ separator ~ start ~ double ~ separator ~ double ~ end ~ end ~ le ~ literal ^^ {
      case _ ~ _ ~ i ~ _ ~ _ ~ lat ~ _ ~ lon ~ _ ~ _ ~ _ ~ d => ElasticGeoDistance(i, d, lat, lon)
    }

  def start: Parser[SQLDelimiter] = "(" ^^ (_ => StartPredicate)
  def end: Parser[SQLDelimiter] = ")" ^^ (_ => EndPredicate)
  def separator: Parser[SQLDelimiter] = "," ^^ (_ => Separator)

  def and: Parser[SQLPredicateOperator] = "(?i)and".r ^^ (_ => AND)
  def or: Parser[SQLPredicateOperator] = "(?i)or".r ^^ (_ => OR)
  def not: Parser[NOT.type] = "(?i)not".r ^^ (_ => NOT)

  def nested: Parser[ElasticOperator] = "(?i)nested".r ^^ (_ => NESTED)
  def child: Parser[ElasticOperator] = "(?i)child".r ^^ (_ => CHILD)
  def parent: Parser[ElasticOperator] = "(?i)parent".r ^^ (_ => PARENT)

  def criteria: Parser[SQLCriteria] =
    start.? ~ (equalityExpression | likeExpression | comparisonExpression | inLiteralExpression | inNumericalExpression | betweenExpression | isNotNullExpression | isNullExpression | distanceExpression) ~ end.? ^^ {
      case _ ~ c ~ _ => c
    }

  @scala.annotation.tailrec
  private def unwrappNested(nested: ElasticNested): SQLCriteria = {
    val c = nested.criteria
    c match {
      case x: ElasticNested => unwrappNested(x)
      case _                => c
    }
  }

  private def unwrappCriteria(criteria: SQLCriteria): SQLCriteria = {
    criteria match {
      case x: ElasticNested => unwrappNested(x)
      case _                => criteria
    }
  }

  private def unwrappPredicate(predicate: SQLPredicate): SQLPredicate = {
    var unwrapp = false
    val _left = predicate.leftCriteria match {
      case x: ElasticNested =>
        unwrapp = true
        unwrappNested(x)
      case l => l
    }
    val _right = predicate.rightCriteria match {
      case x: ElasticNested =>
        unwrapp = true
        unwrappNested(x)
      case r => r
    }
    if (unwrapp)
      SQLPredicate(_left, predicate.operator, _right)
    else
      predicate
  }

  def predicate: Parser[SQLPredicate] = criteria ~ (and | or) ~ not.? ~ criteria ^^ {
    case l ~ o ~ n ~ r => SQLPredicate(l, o, r, n)
  }

  def nestedCriteria: Parser[ElasticRelation] = nested ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticNested(unwrappCriteria(c))
  }
  def nestedPredicate: Parser[ElasticRelation] = nested ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticNested(unwrappPredicate(p))
  }

  def childCriteria: Parser[ElasticRelation] = child ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticChild(unwrappCriteria(c))
  }
  def childPredicate: Parser[ElasticRelation] = child ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticChild(unwrappPredicate(p))
  }

  def parentCriteria: Parser[ElasticRelation] = parent ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticParent(unwrappCriteria(c))
  }
  def parentPredicate: Parser[ElasticRelation] = parent ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticParent(unwrappPredicate(p))
  }

  def alias: Parser[SQLAlias] = "(?i)as".r.? ~ regexAlias.r ^^ { case _ ~ b => SQLAlias(b) }

  def count: Parser[AggregateFunction] = "(?i)count".r ^^ (_ => Count)
  def min: Parser[AggregateFunction] = "(?i)min".r ^^ (_ => Min)
  def max: Parser[AggregateFunction] = "(?i)max".r ^^ (_ => Max)
  def avg: Parser[AggregateFunction] = "(?i)avg".r ^^ (_ => Avg)
  def sum: Parser[AggregateFunction] = "(?i)sum".r ^^ (_ => Sum)

  def _select: Parser[SELECT.type] = "(?i)select".r ^^ (_ => SELECT)

  def _filter: Parser[FILTER.type] = "(?i)filter".r ^^ (_ => FILTER)

  def _from: Parser[FROM.type] = "(?i)from".r ^^ (_ => FROM)

  def _where: Parser[WHERE.type] = "(?i)where".r ^^ (_ => WHERE)

  def _limit: Parser[LIMIT.type] = "(?i)limit".r ^^ (_ => LIMIT)

  def filter: Parser[SQLFilter] = _filter ~> "[" ~> whereCriteria <~ "]" ^^ { case rawTokens =>
    SQLFilter(
      processTokens(rawTokens, None, None, None) match {
        case Some(c) => Some(c)
        case _       => None
      }
    )
  }

  def field: Parser[SQLField] = identifier ~ alias.? ^^ { case i ~ a =>
    SQLField(i, a)
  }

  def aggregate: Parser[SQLAggregate] =
    (min | max | avg | sum | count) ~ start ~ identifier ~ end ~ alias.? ~ filter.? ^^ {
      case agg ~ _ ~ i ~ _ ~ a ~ f => new SQLAggregate(agg, i, a, f)
    }

  def selectAggregates: Parser[SQLSelect] = _select ~ rep1sep(aggregate, separator) ^^ {
    case _ ~ aggregates =>
      new SQLSelectAggregates(aggregates)
  }

  def select: Parser[SQLSelect] = _select ~ rep1sep(field, separator) ^^ { case _ ~ fields =>
    SQLSelect(fields)
  }

  def table: Parser[SQLTable] = identifier ~ alias.? ^^ { case i ~ a => SQLTable(i, a) }

  def from: Parser[SQLFrom] = _from ~ rep1sep(table, separator) ^^ { case _ ~ tables =>
    SQLFrom(tables)
  }

  def allPredicate: SQLParser.Parser[SQLCriteria] =
    nestedPredicate | childPredicate | parentPredicate | predicate

  def allCriteria: SQLParser.Parser[SQLCriteria] =
    nestedCriteria | childCriteria | parentCriteria | criteria

  def whereCriteria: SQLParser.Parser[List[SQLToken]] = rep1(
    allPredicate | allCriteria | start | or | and | end
  )

  def where: Parser[SQLWhere] = _where ~ whereCriteria ^^ { case _ ~ rawTokens =>
    SQLWhere(processTokens(rawTokens, None, None, None))
  }

  def limit: SQLParser.Parser[SQLLimit] = _limit ~ int ^^ { case _ ~ i => SQLLimit(i.value) }

  def tokens: Parser[_ <: SQLSelectQuery] = {
    phrase((selectAggregates | select) ~ from ~ where.? ~ limit.?) ^^ { case s ~ f ~ w ~ l =>
      s match {
        case x: SQLSelectAggregates => new SQLSelectAggregatesQuery(x, f, w, l)
        case _                      => SQLSelectQuery(s, f, w, l)
      }
    }
  }

  def apply(query: String): Either[SQLParserError, SQLSelectQuery] = {
    parse(tokens, query) match {
      case NoSuccess(msg, _) =>
        println(msg)
        Left(SQLParserError(msg))
      case Success(result, _) => Right(result.update())
    }
  }

  @scala.annotation.tailrec
  private def processTokens(
    tokens: List[SQLToken],
    left: Option[SQLCriteria],
    operator: Option[SQLPredicateOperator],
    right: Option[SQLCriteria]
  ): Option[SQLCriteria] = {
    tokens.headOption match {
      case Some(c: SQLCriteria) if left.isEmpty =>
        processTokens(tokens.tail, Some(c), operator, right)

      case Some(c: SQLCriteria) if left.isDefined && operator.isDefined && right.isEmpty =>
        processTokens(tokens.tail, left, operator, Some(c))

      case Some(_: StartDelimiter) => processTokens(tokens.tail, left, operator, right)

      case Some(_: EndDelimiter) if left.isDefined && operator.isDefined && right.isDefined =>
        processTokens(
          tokens.tail,
          Some(SQLPredicate(left.get, operator.get, right.get)),
          None,
          None
        )

      case Some(_: EndDelimiter) => processTokens(tokens.tail, left, operator, right)

      case Some(o: SQLPredicateOperator) if operator.isEmpty =>
        processTokens(tokens.tail, left, Some(o), right)

      case Some(o: SQLPredicateOperator)
          if left.isDefined && operator.isDefined && right.isDefined =>
        processTokens(
          tokens.tail,
          Some(SQLPredicate(left.get, operator.get, right.get)),
          Some(o),
          None
        )

      case None if left.isDefined && operator.isDefined && right.isDefined =>
        Some(SQLPredicate(left.get, operator.get, right.get))

      case None => left

    }
  }
}

trait SQLCompilationError
case class SQLParserError(msg: String) extends SQLCompilationError
