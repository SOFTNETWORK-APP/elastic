package app.softnetwork.elastic.sql

import scala.util.parsing.combinator.RegexParsers

/** Created by smanciot on 27/06/2018.
  */
object SQLParser extends RegexParsers {

  val regexAlias = """\$?[a-zA-Z0-9_]*"""

  val regexRef = """\$[a-zA-Z0-9_]*"""

  def identifier: Parser[SQLIdentifier] =
    "(?i)distinct".r.? ~ (regexRef.r ~ ".").? ~ """[\*a-zA-Z_\-][a-zA-Z0-9_\-\.\[\]]*""".r ^^ {
      case d ~ a ~ str =>
        SQLIdentifier(
          str,
          a match {
            case Some(x) => Some(x._1)
            case _       => None
          },
          d
        )
    }

  def literal: Parser[SQLLiteral] =
    """"[^"]*"""".r ^^ (str => SQLLiteral(str.substring(1, str.length - 1)))

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
    identifier ~ (eq | ne) ~ (boolean | literal | double | int) ^^ { case i ~ o ~ v =>
      SQLExpression(i, o, v)
    }
  def likeExpression: Parser[SQLExpression] = identifier ~ like ~ literal ^^ { case i ~ o ~ v =>
    SQLExpression(i, o, v)
  }
  def comparisonExpression: Parser[SQLExpression] =
    identifier ~ (ge | gt | le | lt) ~ (double | int | literal) ^^ { case i ~ o ~ v =>
      SQLExpression(i, o, v)
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
      case _ ~ c ~ _ =>
        c match {
          case x: SQLExpression if x.columnName.nested  => ElasticNested(x)
          case y: SQLIn[_, _] if y.columnName.nested    => ElasticNested(y)
          case z: SQLBetween if z.columnName.nested     => ElasticNested(z)
          case n: SQLIsNull if n.columnName.nested      => ElasticNested(n)
          case nn: SQLIsNotNull if nn.columnName.nested => ElasticNested(nn)
          case _                                        => c
        }
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

  def alias: Parser[SQLAlias] = "(?i)as".r ~ regexAlias.r ^^ { case _ ~ b => SQLAlias(b) }

  def count: Parser[SQLFunction] = "(?i)count".r ^^ (_ => SQLCount)
  def min: Parser[SQLFunction] = "(?i)min".r ^^ (_ => SQLMin)
  def max: Parser[SQLFunction] = "(?i)max".r ^^ (_ => SQLMax)
  def avg: Parser[SQLFunction] = "(?i)avg".r ^^ (_ => SQLAvg)
  def sum: Parser[SQLFunction] = "(?i)sum".r ^^ (_ => SQLSum)

  def _select: Parser[SELECT.type] = "(?i)select".r ^^ (_ => SELECT)

  def _filter: Parser[FILTER.type] = "(?i)filter".r ^^ (_ => FILTER)

  def _from: Parser[FROM.type] = "(?i)from".r ^^ (_ => FROM)

  def _where: Parser[WHERE.type] = "(?i)where".r ^^ (_ => WHERE)

  def _limit: Parser[LIMIT.type] = "(?i)limit".r ^^ (_ => LIMIT)

  def countFilter: Parser[SQLFilter] = _filter ~> "[" ~> whereCriteria <~ "]" ^^ { case rawTokens =>
    SQLFilter(
      processTokens(rawTokens, None, None, None) match {
        case Some(c) => Some(unwrappCriteria(c))
        case _       => None
      }
    )
  }

  def countField: Parser[SQLCountField] =
    count ~ start ~ identifier ~ end ~ alias.? ~ countFilter.? ^^ { case _ ~ _ ~ i ~ _ ~ a ~ f =>
      new SQLCountField(i, a, f)
    }

  def field: Parser[SQLField] =
    (min | max | avg | sum).? ~ start.? ~ identifier ~ end.? ~ alias.? ^^ {
      case f ~ _ ~ i ~ _ ~ a => SQLField(f, i, a)
    }

  def selectCount: Parser[SQLSelect] = _select ~ rep1sep(countField, separator) ^^ {
    case _ ~ fields => new SQLSelectCount(fields)
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
    phrase((selectCount | select) ~ from ~ where.? ~ limit.?) ^^ { case s ~ f ~ w ~ l =>
      s match {
        case x: SQLSelectCount => new SQLCountQuery(x, f, w, l)
        case _                 => SQLSelectQuery(s, f, w, l)
      }
    }
  }

  def apply(query: String): Either[SQLParserError, SQLSelectQuery] = {
    parse(tokens, query) match {
      case NoSuccess(msg, _) =>
        println(msg)
        Left(SQLParserError(msg))
      case Success(result, _) => Right(result)
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
