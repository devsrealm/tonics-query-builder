<?php

namespace Devsrealm\TonicsQueryBuilder;

use PDO;

class TonicsQuery {

    private ?TonicsQueryBuilder $tonicsQueryBuilder = null;
    private string $sqlString = '';
    private array $params = [];
    private string $lastEmittedType = '';

    private int $pdoFetchType = PDO::FETCH_OBJ;
    private ?int $rowCount = null;
    private array $urlParams = [];

    public function __construct(TonicsQueryBuilder $tonicsQueryBuilder = null)
    {
        if ($tonicsQueryBuilder){
            $this->tonicsQueryBuilder = $tonicsQueryBuilder;
        }

        $params = [];
        parse_str($_SERVER['QUERY_STRING'], $params);
        $this->setURLParams($params);
    }

    const Equals = 'Equals';
    const NotEquals = 'NotEquals';
    const LessThan = 'LessThan';
    const LessThanOrEqualTo = 'LessThanOrEqualTo';
    const NullSafeEqualOperator = 'NullSafeEqualOperator';
    const GreaterThan = 'GreaterThan';
    const GreaterThanOrEqualTo = 'GreaterThanOrEqualTo';

    private array $validWhereOP = [
        '=' => self::Equals,
        '!=' => self::NotEquals,
        '<' => self::LessThan,
        '<=' => self::LessThanOrEqualTo,
        '<=>' => self::NullSafeEqualOperator,
        '>' => self::GreaterThan,
        '>=' => self::GreaterThanOrEqualTo,
    ];

    public function Equals(): string
    {
        return '=';
    }

    public function NotEquals(): string
    {
        return '!=';
    }

    public function LessThan(): string
    {
        return '<';
    }

    public function LessThanOrEqualTo(): string
    {
        return '<=';
    }

    public function NullSafeEqualOperator(): string
    {
        return '<=>';
    }

    public function GreaterThan(): string
    {
        return '>';
    }

    public function GreaterThanOrEqualTo(): string
    {
        return '>=';
    }

    /**
     * @return int
     */
    public function getPdoFetchType(): int
    {
        return $this->pdoFetchType;
    }

    /**
     * @param int $pdoFetchType
     * @return TonicsQuery
     */
    public function setPdoFetchType(int $pdoFetchType): TonicsQuery
    {
        $this->pdoFetchType = $pdoFetchType;
        return $this;
    }

    /**
     * @return int|null
     */
    public function getRowCount(): ?int
    {
        return $this->rowCount;
    }

    /**
     * @param int|null $rowCount
     * @return TonicsQuery
     */
    public function setRowCount(?int $rowCount): TonicsQuery
    {
        $this->rowCount = $rowCount;
        return $this;
    }

    /**
     * @return TonicsQueryBuilder|null
     */
    public function getTonicsQueryBuilder(): ?TonicsQueryBuilder
    {
        return $this->tonicsQueryBuilder;
    }

    /**
     * @param TonicsQueryBuilder|null $tonicsQueryBuilder
     * @return TonicsQuery
     */
    public function setTonicsQueryBuilder(?TonicsQueryBuilder $tonicsQueryBuilder): TonicsQuery
    {
        $this->tonicsQueryBuilder = $tonicsQueryBuilder;
        return $this;
    }

    /**
     * @return string
     */
    public function getSqlString(): string
    {
        return $this->sqlString;
    }

    /**
     * @param string $sqlString
     * @return TonicsQuery
     */
    public function setSqlString(string $sqlString): TonicsQuery
    {
        $this->sqlString = $sqlString;
        return $this;
    }

    /**
     * @param string $sqlString
     * @return $this
     */
    protected function addSqlString(string $sqlString): static
    {
        $this->sqlString .= $sqlString . " ";
        return $this;
    }

    /**
     * @return array
     */
    public function getParams(): array
    {
        return $this->params;
    }

    /**
     * @param array $params
     * @return TonicsQuery
     */
    public function setParams(array $params): TonicsQuery
    {
        $this->params = $params;
        return $this;
    }

    /**
     * @param $param
     * @return $this
     */
    public function addParam($param): static
    {
        $this->params[] = $param;
        return $this;
    }

    /**
     * @param array $params
     * @return $this
     */
    public function addParams(array $params): static
    {
        foreach ($params as $param){
            $this->params[] = $param;
        }
        return $this;
    }

    /**
     * @return string
     */
    public function getLastEmittedType(): string
    {
        return $this->lastEmittedType;
    }

    /**
     * @param string $lastEmittedType
     * @return TonicsQuery
     */
    public function setLastEmittedType(string $lastEmittedType): TonicsQuery
    {
        $this->lastEmittedType = $lastEmittedType;
        return $this;
    }

    public function isLastEmitted(string $type): bool
    {
        return strtolower($this->lastEmittedType) === strtolower($type);
    }

    protected function isWhereOP(string $opValue): bool
    {
        return key_exists($opValue, $this->validWhereOP);
    }

    /**
     * @param string $opValue
     * @return string
     * @throws \Exception
     */
    protected function getWhereOP(string $opValue): string
    {
        if ($this->isWhereOP($opValue)){
            $whereFunc = $this->validWhereOP[$opValue];
            return $this->$whereFunc();
        }

        throw new \Exception("Invalid Operator $opValue");
    }

    /**
     * If you have multiple fluent Select method, e.g:
     *
     * `Select()->Select()` the second Select arg should be a TonicsQuery Object, and it would be subqueried,
     * besides, you do not need to add SELECT in the object, it would be added automatically.
     * @param string|TonicsQuery $select
     * @return $this
     * @throws \Exception
     */
    public function Select(string|TonicsQuery $select = ''): static
    {
        if ($this->isLastEmitted('SELECT')){
            if (is_object($select)){
                $this->validateNewInstanceOfTonicsQuery($select);
                $this->addSqlString("( SELECT {$select->getSqlString()} )");
                $this->params = [...$this->params, ...$select->getParams()];
            }
            throw new \Exception("Last emitted type was select, the current select arg should be a TonicsQuery Object");
        } else {
            $this->setLastEmittedType('SELECT');
            $this->addSqlString("SELECT $select");
        }

        return $this;
    }

    /**
     * @param string|TonicsQuery $table
     * @return $this
     * @throws \Exception
     */
    public function From(string|TonicsQuery $table): static
    {
        $this->setLastEmittedType('FROM');
        if (is_object($table)){
            $this->validateNewInstanceOfTonicsQuery($table);
            $this->addSqlString("FROM ( {$table->getSqlString()} )");
            $this->params = [...$this->params, ...$table->getParams()];
        } else {
            $this->addSqlString("FROM $table");
        }

        return $this;
    }

    /**
     * @param string $ifWhereUse
     * @return string
     */
    protected function getWhere(string $ifWhereUse = 'AND'): string
    {
        $addWhere = 'WHERE';
        if ($this->isLastEmitted('WHERE')){
            $addWhere = $ifWhereUse;
        }
        $this->setLastEmittedType('WHERE');
        return $addWhere;
    }

    /**
     * If $value is TonicQuery object, it would be converted to a subquery
     * @param string $col
     * @param string $op
     * @param $value
     * @return $this
     * @throws \Exception
     */
    public function Where(string $col, string $op, $value): static
    {
        $op = $this->getWhereOP($op);
        if ($value instanceof TonicsQuery){
            $this->validateNewInstanceOfTonicsQuery($value);
            $this->addSqlString("{$this->getWhere()} $col $op ( {$value->getSqlString()} )");
            $this->addParams($value->getParams());
        } else {
            $this->addSqlString("{$this->getWhere()} $col $op ?");
            $this->addParam($value);
        }

        return $this;
    }

    /**
     * @param string $col
     * @param TonicsQuery $min
     * @param TonicsQuery $max
     * @return $this
     * @throws \Exception
     */
    public function WhereBetween(string $col, TonicsQuery $min, TonicsQuery $max): static
    {
        $this->validateNewInstanceOfTonicsQuery($min);
        $this->validateNewInstanceOfTonicsQuery($max);

        $this->addSqlString("{$this->getWhere()} $col BETWEEN {$min->getSqlString()} AND {$max->getSqlString()}");
        $this->addParams($min->getParams());
        $this->addParams($max->getParams());

        return $this;
    }

    /**
     * @param string $col
     * @param $value
     * @return $this
     * @throws \Exception
     */
    public function WhereEquals(string $col, $value): static
    {
        return $this->Where($col, '=', $value);
    }

    /**
     * @param string $col
     * @param string $op
     * @param string $value
     * @return $this
     * @throws \Exception
     */
    public function WhereDate(string $col, string $op, string $value): static
    {
        $op = $this->getWhereOP($op);
        $this->addSqlString("{$this->getWhere()} DATE($col) $op ?");
        $this->addParam($value);
        return $this;
    }

    /**
     * @param string $date
     * @param string $format
     * @return $this
     */
    public function WhereDateFormat(string $date, string $format = '%Y-%m-%d %H:%i:%s'): static
    {
        $this->addSqlString("{$this->getWhere()} DATE_FORMAT(?, ?)");
        $this->addParams([$date, $format]);
        return $this;
    }

    /**
     * @param string $col
     * @param string $op
     * @param string $value
     * @return $this
     * @throws \Exception
     */
    public function WhereTime(string $col, string $op, string $value): static
    {
        $op = $this->getWhereOP($op);
        $this->addSqlString("{$this->getWhere()} TIME($col) $op ?");
        $this->addParam($value);
        return $this;
    }

    /**
     * @param string $col
     * @return $this
     */
    public function WhereNull(string $col): static
    {
        $this->addSqlString("{$this->getWhere()} $col IS NULL");
        return $this;
    }

    /**
     * @param string $col
     * @return $this
     */
    public function OrWhereNull(string $col): static
    {
        $this->addSqlString("{$this->getWhere('OR')} $col IS NULL");
        return $this;
    }

    /**
     * @param string $col
     * @return $this
     */
    public function WhereNotNull(string $col): static
    {
        $this->addSqlString("{$this->getWhere()} $col IS NOT NULL");
        return $this;
    }

    /**
     * @param string $col
     * @return $this
     */
    public function OrWhereNotNull(string $col): static
    {
        $this->addSqlString("{$this->getWhere('OR')} $col IS NOT NULL");
        return $this;
    }

    /**
     * @param string $col
     * @return $this
     */
    public function WhereFalse(string $col): static
    {
        $this->addSqlString("{$this->getWhere()} $col = FALSE");
        return $this;
    }

    /**
     * @param string $col
     * @return $this
     */
    public function OrWhereFalse(string $col): static
    {
        $this->addSqlString("{$this->getWhere('OR')} $col = FALSE");
        return $this;
    }

    /**
     * @param string $col
     * @return $this
     */
    public function WhereTrue(string $col): static
    {
        $this->addSqlString("{$this->getWhere()} $col = TRUE");
        return $this;
    }

    /**
     * @param string $col
     * @return $this
     */
    public function OrWhereTrue(string $col): static
    {
        $this->addSqlString("{$this->getWhere('OR')} $col = TRUE");
        return $this;
    }

    /**
     * @param string $col
     * @param $value
     * @param string $type
     * @return $this
     * @throws \Exception
     */
    protected function WhereIn_NotIn(string $col, $value, string $type = 'IN'): static
    {
        if ($value instanceof \stdClass){
            $value = (array)$value;
        }

        $addWhere = $this->getWhere();

        if (is_array($value) && array_is_list($value)){
            $qmark = $this->returnRequiredQuestionMarks($value);
            $this->addSqlString("{$addWhere} $col $type($qmark)");
            $this->addParams($value);
        }

        if ($value instanceof TonicsQuery){
            $this->validateNewInstanceOfTonicsQuery($value);
            $this->addSqlString("{$addWhere} $col $type ( {$value->getSqlString()} )");
            $this->addParams($value->getParams());
        }

        return $this;
    }

    /**
     * @throws \Exception
     */
    public function WhereIn(string $col, $value): static
    {
        return $this->WhereIn_NotIn($col, $value, 'IN');
    }

    /**
     * @throws \Exception
     */
    public function WhereNotIn(string $col, $value): static
    {
        return $this->WhereIn_NotIn($col, $value, 'NOT IN');
    }

    /**
     * @param string $col
     * @param string $value
     * @return $this
     */
    public function WhereEnds(string $col, string $value): static
    {
        $this->addSqlString("{$this->getWhere()} $col LIKE CONCAT(?, ?)");
        $this->addParams(['%', $value]);
        return $this;
    }

    /**
     * @param string $col
     * @param string $value
     * @return $this
     */
    public function WhereStarts(string $col, string $value): static
    {
        $this->addSqlString("{$this->getWhere()} $col LIKE CONCAT(?, ?)");
        $this->addParams([$value, '%']);
        return $this;
    }

    /**
     * @param string $col
     * @param string $value
     * @return $this
     */
    public function WhereLike(string $col, string $value): static
    {
        $this->addSqlString("{$this->getWhere()} $col LIKE CONCAT(?, ?, ?)");
        $this->addParams(['%', $value, '%']);
        return $this;
    }

    /**
     * @param string $string
     * @return $this
     */
    public function As(string $string): static
    {
        $this->setLastEmittedType('AS');
        $this->addSqlString("AS $string");
        return $this;
    }

    /**
     * @param int $number
     * @return TonicsQuery
     */
    public function Limit(int $number): static
    {
        $this->setLastEmittedType('LIMIT');
        $this->addSqlString("LIMIT ?");
        $this->addParam($number);
        return $this;
    }

    /**
     * @param int $number
     * @return TonicsQuery
     */
    public function Take(int $number): static
    {
        return $this->Limit($number);
    }

    /**
     * @param int $number
     * @return TonicsQuery
     */
    public function Offset(int $number): static
    {
        $this->setLastEmittedType('OFFSET');
        $this->addSqlString("OFFSET ?");
        $this->addParam($number);
        return $this;
    }

    /**
     * @param int $number
     * @return TonicsQuery
     */
    public function Skip(int $number): static
    {
        return $this->Offset($number);
    }

    /**
     * @param string $name
     * @return $this
     */
    public function Count(string $name = '*'): static
    {
        $this->setLastEmittedType('COUNT');
        $this->addSqlString("COUNT(?)");
        $this->addParam($name);
        return $this;
    }


    /**
     * @param string $column
     * @return $this
     */
    public function OrderBy(string $column): static
    {
        $orderBy = 'ORDER BY ';
        if($this->isLastEmitted('ORDER BY')){
            $orderBy = ', ';
        }
        $this->setLastEmittedType('ORDER BY');
        $this->addSqlString("$orderBy$column");
        return $this;
    }

    /**
     * @param $value
     * @return $this
     * @throws \Exception
     */
    public function In($value): static
    {
        if ($value instanceof \stdClass){
            $value = (array)$value;
        }

        if (is_array($value) && array_is_list($value)){
            $qmark = $this->returnRequiredQuestionMarks($value);
            $this->addSqlString("IN($qmark)");
            $this->addParams($value);
        }elseif (is_object($value)){
            $this->validateNewInstanceOfTonicsQuery($value);
            $this->addSqlString("IN ( {$value->getSqlString()} )");
            $this->addParams($value->getParams());
        } else {
            throw new \Exception("In argument can only be an Array, Stdclass and a TonicsQuery Object");
        }

        return $this;
    }

    /**
     * @param string $column
     * @return $this
     */
    public function OrderByDesc(string $column): static
    {
        $orderBy = 'ORDER BY ';
        if($this->isLastEmitted('ORDER BY')){
            $orderBy = ', ';
        }

        $this->setLastEmittedType('ORDER BY');
        $this->addSqlString("$orderBy$column DESC");
        return $this;
    }

    /**
     * @param string $column
     * @return $this
     */
    public function OrderByAsc(string $column): static
    {
        $orderBy = 'ORDER BY ';
        if($this->isLastEmitted('ORDER BY')){
            $orderBy = ', ';
        }

        $this->setLastEmittedType('ORDER BY');
        $this->addSqlString("$orderBy$column ASC");
        return $this;
    }

    /**
     * @param string $column
     * @return $this
     */
    public function GroupBy(string $column): static
    {
        $groupBy = 'GROUP BY ';
        if($this->isLastEmitted('GROUP BY')){
            $groupBy = ', ';
        }

        $this->setLastEmittedType('GROUP BY');
        $this->addSqlString("$groupBy$column");
        return $this;
    }

    /**
     * @param $first
     * @param $op
     * @param $value
     * @return $this
     * @throws \Exception
     */
    public function Having($first, $op, $value): static
    {
        $op = $this->getWhereOP($op);
        $having = 'HAVING';
        if($this->isLastEmitted('HAVING')){
            $having = 'AND';
        }
        $this->setLastEmittedType('HAVING');
        $this->addSqlString("$having $first $op ?");
        $this->addParam($value);
        return $this;
    }

    /**
     * @param string $date
     * @param string $format
     * @return $this
     */
    public function DateFormat(string $date, string $format = '%Y-%m-%d %H:%i:%s'): static
    {
        $this->addSqlString("DATE_FORMAT(?, ?)");
        $this->addParams([$date, $format]);
        return $this;
    }

    /**
     * @throws \Exception
     */
    private function UnionRelated(TonicsQuery $subQuery, string $type = 'UNION'): static
    {
        $this->validateNewInstanceOfTonicsQuery($subQuery);
        $this->addSqlString("$type ( {$subQuery->getSqlString()} )");
        $this->addParams($subQuery->getParams());
        return $this;
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function Union(TonicsQuery $subQuery): static
    {
        return $this->UnionRelated($subQuery);
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function UnionAll(TonicsQuery $subQuery): static
    {
        return $this->UnionRelated($subQuery, 'UNION ALL');
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function Intersect(TonicsQuery $subQuery): static
    {
        return $this->UnionRelated($subQuery, 'INTERSECT');
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function IntersectAll(TonicsQuery $subQuery): static
    {
        return $this->UnionRelated($subQuery, 'INTERSECT ALL');
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function Except(TonicsQuery $subQuery): static
    {
        return $this->UnionRelated($subQuery, 'EXCEPT');
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function ExceptAll(TonicsQuery $subQuery): static
    {
        return $this->UnionRelated($subQuery, 'EXCEPT ALL');
    }

    public function With(string $cteName, TonicsQuery $cteBody, bool $recursive = false)
    {
        $with = 'WITH';
        if($this->isLastEmitted('WITH')){
            $with = ',';
        }
        $recursiveName = '';
        if($recursive){
            $recursiveName = 'RECURSIVE';
        }
        $this->setLastEmittedType('WITH');
        $this->addSqlString("$with $recursiveName $cteName AS ( {$cteBody->getSqlString()} )");
        $this->addParams($cteBody->getParams());
        return $this;
    }

    /**
     * @param string $table
     * @param string $col
     * @param string $col2
     * @param string $op
     * @param string $type
     * @return $this
     * @throws \Exception
     */
    private function JoinRelative(string $table, string $col, string $col2, string $op = '=', string $type = 'INNER JOIN'): static
    {
        $this->setLastEmittedType($type);
        $op = $this->getWhereOP($op);
        $this->addSqlString("$type $table ON $col $op $col2");
        return $this;
    }


    /**
     * @param $table
     * @param $col
     * @param $col2
     * @param string $op
     * @return $this
     * @throws \Exception
     */
    public function Join($table, $col, $col2, string $op = '='): static
    {
        return $this->JoinRelative($table, $col, $col2, $op);
    }

    /**
     * @param $table
     * @param $col
     * @param $col2
     * @param string $op
     * @return $this
     * @throws \Exception
     */
    public function LeftJoin($table, $col, $col2, string $op = '='): static
    {
        return $this->JoinRelative($table, $col, $col2, $op, 'LEFT JOIN');
    }

    /**
     * @param $table
     * @param $col
     * @param $col2
     * @param string $op
     * @return $this
     * @throws \Exception
     */
    public function RightJoin($table, $col, $col2, string $op = '='): static
    {
        return $this->JoinRelative($table, $col, $col2, $op, 'RIGHT JOIN');
    }


    public function Or(): string
    {
        return 'OR ';
    }

    public function And(): string
    {
        return 'AND ';
    }

    public function addRawString(string $raw): static
    {
        $this->addSqlString($raw);
        return $this;
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function SubQuery(TonicsQuery $subQuery): static
    {
        $this->setLastEmittedType('SubQuery');
        $this->validateNewInstanceOfTonicsQuery($subQuery);
        $this->addSqlString("( {$subQuery->getSqlString()} )");
        $this->addParams($subQuery->getParams());
        return $this;
    }

    /**
     * @param string $jsonDoc
     * @param string $path
     * @param string $accessor
     * @return $this
     */
    public function JsonExtract(string $jsonDoc, string $path, string $accessor = '$.'): static
    {
        $this->setLastEmittedType('JSON_EXTRACT');
        $this->addSqlString("JSON_EXTRACT($jsonDoc, ?)");
        $this->addParam($accessor . $path);
        return $this;
    }

    /**
     * @param string $jsonDoc
     * @param ...$path
     * @return $this
     */
    public function JsonSet(string $jsonDoc, ...$path): static
    {
        $this->setLastEmittedType('JSON_SET');
        $mark = $this->returnRequiredQuestionMarks($path);
        $this->addSqlString("JSON_SET($jsonDoc, $mark)");
        $this->addParams($path);
        return $this;
    }

    /**
     * @param string $jsonDoc
     * @param string $path
     * @param string $accessor
     * @return $this
     */
    public function JsonExist(string $jsonDoc, string $path, string $accessor = '$.'): static
    {
        $this->setLastEmittedType('JSON_EXIST');
        $this->addSqlString("JSON_EXIST($jsonDoc, ?)");
        $this->addParam($accessor. $path);
        return $this;
    }

    /**
     * If the jsonDocs should use a parameter binding, then you do it yourself
     * @param string $jsonDoc
     * @param string $jsonDoc2
     * @return $this
     */
    public function JsonMergePatch(string $jsonDoc, string $jsonDoc2): static
    {
        $this->lastEmittedType = 'JSON_MERGE_PATCH';
        $this->addSqlString("JSON_MERGE_PATCH($jsonDoc $jsonDoc2");
        return $this;
    }

    /**
     * @param int $tableRows
     * The total number of rows
     * @param \Closure $callback
     * You would get the perPage and offset in the callback parameter which you can then do some queries with and return the result of the query....
     * So, I can take care of the rest of the pagination implementation
     * @param int $perPage
     * How many rows to retrieve per page when paginating
     * @param string $pageName
     * The page url query name, i.e ?page=20 (the pagename is `page`, which tells me what page to move to in the pagination window)
     * @return object|null
     * @throws \Exception
     */
    public function Paginate(
        int $tableRows,
        \Closure $callback,
        int $perPage = 5,
        string $pageName = 'page',
    ): ?object
    {

        # The reason for doing ($tableRows / $perPage) is to determine the number of total pages we can paginate through
        $totalPages = (int)ceil($tableRows / $perPage);

        $params = $this->getURLParams();
        # current page - The page the user is currently on, if we can't find the page number, we default to the first page
        $page = 1;
        if(isset($params[$pageName])){
            $page = filter_var($params[$pageName], FILTER_SANITIZE_NUMBER_INT);
        }

        #
        # Get the Offset based on the current page
        # The offset would determine the numbers of rows to skip before
        # returning result.
        $offset = ($page - 1) * $perPage;

        $result = $callback($perPage, $offset);
        if ($result) {
            #
            # ARRANGE THE PAGINATION RESULT
            return $this->arrangePagination(
                sqlResult: $result,
                page: $page,
                totalPages: $totalPages,
                perPage: $perPage, pageName: $pageName);
        }
        return null;
    }

    /**
     * The `SimplePaginate()` method uses a subQuery to get the count of the rows,
     * and then passes that to the Paginate method, you can use this most of the time
     * since most RDMS would rewrite the query without the SubQuery, so, you are fine.
     *
     * <br>
     * Note: Feel free to use the `Paginate()` method if you want to do things your way.
     * @param int $perPage
     * @param string $pageName
     * @return object|null
     * @throws \Exception
     */
    public function SimplePaginate(int $perPage = 10, string $pageName = 'page'): ?object
    {
        $newQuery = $this->Q();
        $tableRows = $newQuery->Select('')->Count()
            ->As('`rows`')->From(" ( {$this->getSqlString()} ) ")
            ->As('count')->addParams($this->getParams())->FetchFirst();

        if (!isset($tableRows->rows)){
            $tableRows = 0;
        } else {
            $tableRows = $tableRows->rows;
        }

        return $this->paginate($tableRows, function ($perPage, $offset){
            return  $this->Take($perPage)->Skip($offset)->FetchResult();
        }, $perPage, $pageName);
    }

    /**
     * @param string $table
     * @param string $col
     * @param TonicsQuery $select
     * @return bool
     */
    public function InsertSelect(string $table, string $col, TonicsQuery $select): bool
    {
        $sql = "INSERT INTO $table ($col) {$select->getSqlString()}";
        $stmt = $this->getPdo()->prepare($sql);
        $stmt->execute($select->getParams());
        $this->setRowCount($stmt->rowCount());
        return $this->getRowCount() > 0;
    }

    /**
     * For Insertion or Batch Insertion, use below format for single insert:
     *
     * `["genre_name" => "Acoustic", "genre_slug" => "acoustic", "genre_description" => "Acoustic"]`
     *
     * <br>
     * and use below format for batch insertion:
     * ```
     * [
     *  ["genre_name" => "Acoustic", "genre_slug" => "acoustic", "genre_description" => "Acoustic"],
     *  ["genre_name" => "Afrobeat", "genre_slug" => "afrobeat", "genre_description" => "Afrobeat"]
     * ]
     *  array_key is the table_column, and array_value is the value of the column
     * ```
     * @param string $table
     * name of the table
     * @param array $data
     * @param int $chunkInsertRate
     * @return bool
     */
    public function Insert(string $table, array $data, int $chunkInsertRate = 1000): bool
    {

        if (empty($data)) return false;

        if (!is_array(reset($data))) $data = [$data];

        # This gets the array_keys of the first element in the array
        # which would act as the columns of all the array
        $getColumns = array_keys(reset($data));
        # e.g, "`column1`,`column1`,`column1`",
        $delimitedColumns = $this->escapeDelimitedColumns($getColumns);

        $rowCount = 0;

        #
        # Chunking Operation Begins
        #
        foreach (array_chunk($data, $chunkInsertRate) as $toInsert){
            $numberOfQ = $this->returnRequiredQuestionMarksSurroundedWithParenthesis($toInsert);
            # we would throw away the keys in the multidimensional array and flatten it into 1d array
            $flattened = iterator_to_array(new \RecursiveIteratorIterator(new \RecursiveArrayIterator($toInsert)), 0);
            # SQL
            $sql = "INSERT INTO $table ($delimitedColumns) VALUES $numberOfQ;";
            # Prepare Statement and execute it ;P
            $stmt = $this->getPdo()->prepare($sql);
            $stmt->execute($flattened);
            $rowCount += $stmt->rowCount();
        }

        $this->setRowCount($rowCount);
        return $this->getRowCount() > 0;
    }

    /**
     * This Either Insert or Update New Record if matched by unique or primary key
     * @param string $table
     * Table name
     * @param array $data
     * Data To Insert
     * @param array $update
     * Update Keys
     * @param int $chunkInsertRate
     * How many records to insert at a time, the default is okay, but you can experiment with more
     * @return false
     * @throws \Exception
     */
    public function InsertOnDuplicate(string $table, array $data, array $update, int $chunkInsertRate = 1000): bool
    {

        if (empty($data)) return false;

        if (!is_array(reset($data))) $data = [$data];

        #
        # VALIDATION AND DELIMITATION FOR $data
        #
        $getColumns = array_keys(reset($data));
        $delimitedColumns = $this->escapeDelimitedColumns($getColumns);

        $rowCount = 0;

        #
        # Chunking Operation Begins
        #
        foreach (array_chunk($data, $chunkInsertRate) as $toInsert){
            $numberOfQ = $this->returnRequiredQuestionMarksSurroundedWithParenthesis($toInsert);

            if (!is_array(reset($update))){
                $update = [$update];
            }
            $update = array_values(reset($update));
            #
            # SQL PREPARE, INSERTION AND DATA RETURNING
            #
            $delimitedForInsertOnDuplicate = $this->delimitedForInsertOnDuplicate($update);
            $flattened = iterator_to_array(new \RecursiveIteratorIterator(new \RecursiveArrayIterator($toInsert)), 0);

            $sql = "INSERT INTO $table ($delimitedColumns) VALUES $numberOfQ ON DUPLICATE KEY UPDATE $delimitedForInsertOnDuplicate";
            $stmt = $this->getPdo()->prepare($sql);
            $stmt->execute($flattened);
            $rowCount += $stmt->rowCount();
        }

        $this->setRowCount($rowCount);
        return $this->getRowCount() > 0;
    }

    /**
     * Insert and Return Data specified in $return.
     *
     * <br>
     * Note: The $primaryKey is a way of providing an alternative approach to InsertReturning for other RDBMS
     * that doesn't support it,
     * @param string $table
     * @param array $data
     * @param array $return
     * @param string $primaryKey
     * @return \stdClass|bool
     */
    public function InsertReturning(string $table, array $data, array $return, string $primaryKey): mixed
    {
        if (empty($data)) return false;

        if (!is_array(reset($data))) $data = [$data];

        if (!is_array(reset($return))) $return = [$return];

        #
        # VALIDATION AND DELIMITATION FOR RETURNING
        #
        $getReturningColumns = (array)array_values(reset($return));
        $delimitedReturningColumns = $this->escapeDelimitedColumns($getReturningColumns);

        #
        # VALIDATION AND DELIMITATION FOR THE ACTUAL COLUMN
        #
        $getColumns = array_keys(reset($data));
        # e.g, "`column1`,`column1`,`column1`",
        $delimitedColumns = $this->escapeDelimitedColumns($getColumns);
        $numberOfQ = $this->returnRequiredQuestionMarksSurroundedWithParenthesis($data);

        #
        # SQL PREPARE, INSERTION AND DATA RETURNING
        #
        $sql = "INSERT INTO $table ($delimitedColumns) VALUES $numberOfQ RETURNING $delimitedReturningColumns";
        $stmt = $this->getPdo()->prepare($sql);
        $flattened = iterator_to_array(new \RecursiveIteratorIterator(new \RecursiveArrayIterator($data)), 0);
        $stmt->execute($flattened);
        $this->setRowCount($stmt->rowCount());
        return $stmt->fetch($this->getPdoFetchType());
    }

    /**
     * Update statement, this won't update anything,
     * it only adds the `UPDATE $table` statement to the SQLString.
     *
     * <br>
     * Update statement can be followed by the `Set()` method
     * @param string $table
     * @return $this
     */
    public function Update(string $table): static
    {
        $this->lastEmittedType = 'UPDATE';
        $this->addSqlString("UPDATE $table ");
        return $this;
    }

    /**
     * @param $col
     * @param string|TonicsQuery $value
     * @return $this
     * @throws \Exception
     */
    public function Set($col, string|TonicsQuery $value): static
    {
        $set = 'SET';
        if($this->isLastEmitted('SET')){
            $set = ',';
        }
        $this->lastEmittedType = 'SET';
        if (is_object($value)){
            $this->validateNewInstanceOfTonicsQuery($value);
            $this->addSqlString("$set $col = {$value->getSqlString()} ");
            $this->addParams($value->getParams());
        } else {
            $this->addSqlString("$set $col = ? ");
            $this->addParam($value);
        }

        return $this;
    }

    /**
     * @param string $table
     * @param array $updateChanges
     * @param TonicsQuery $whereCondition
     * @return mixed
     * @throws \Exception
     */
    public function FastUpdate(string $table, array $updateChanges, TonicsQuery $whereCondition): mixed
    {
        if (empty($table)){
            throw new \Exception("Table $table must be non empty string");
        }

        $updateString = "UPDATE $table SET";
        $params = [];

        $pre = [];
        foreach ($updateChanges as $col => $v) {
            if ($v === null) {
                $pre []= " $col = NULL";
            } elseif (is_bool($v)) {
                if ($v === true) {
                    $pre [] = " $col = TRUE ";
                }else {
                    $pre [] = " $col = FALSE ";
                }
            } else {
                $pre [] = " $col = ?";
                $params[] = $v;
            }
        }

        $updateString .= implode(', ', $pre) . " {$whereCondition->getSqlString()} ";
        $params = [...$params, ...$whereCondition->getParams()];

        $stmt = $this->getPdo()->prepare($updateString);
        $stmt->execute($params);
        $this->setRowCount($stmt->rowCount());
        return $this->getRowCount() > 0;
    }


    /**
     * Delete statement, this won't delete anything,
     * it only adds the `DELETE FROM $table` statement to the SQLString.
     *
     * <br>
     * To delete, use the `FastDelete()` method
     * @param string $table
     * @return $this
     */
    public function Delete(string $table): static
    {
        $this->lastEmittedType = 'DELETE';
        $this->addSqlString("DELETE FROM $table");
        return $this;
    }

    /**
     * @param string $table
     * Table you wanna delete from
     * @param TonicsQuery $whereCondition
     * The where condition
     * @return bool|int
     * @throws \Exception
     */
    public function FastDelete(string $table, TonicsQuery $whereCondition): bool|int
    {
        if (empty($table)){
            throw new \Exception("Table $table must be non empty string");
        }

        $deleteString = "DELETE FROM $table ";
        $whereConditionString = $whereCondition->getSqlString();
        if (empty($whereConditionString)){
            return 0;
        }

        $deleteString .= $whereConditionString;
        $stmt = $this->getPdo()->prepare($deleteString);
        $stmt->execute($whereCondition->getParams());
        $this->setRowCount($stmt->rowCount());
        return $this->getRowCount() > 0;

    }

    /**
     * For RAW SQL;
     *
     * <br>
     * Note: You should add the question mark if there is any, and use the addParam or addParams to add the parameter
     * @param string $raw
     * @return $this
     */
    public function Raw(string $raw): static
    {
        $this->addSqlString($raw);
        return $this;
    }

    #
    # THE BELOW ARE HELPERS
    #

    /**
     * Get a new instance of TonicsQuery
     * @return TonicsQuery
     */
    public function Q(): TonicsQuery
    {
        return $this->getTonicsQueryBuilder()->getNewQuery();
    }


    /**
     * @param array $data
     * @return string
     */
    protected function returnRequiredQuestionMarksSurroundedWithParenthesis(array $data): string
    {
        # Returns question marks, e.g, if the num column of what we would be inserting is 3, it returns "?,?,?"
        $numberOfQ = $this->returnRequiredQuestionMarks($data);
        # surround the question mark in (?,?,?), and duplicate it across rows we'll be inserting
        return implode(',', array_fill(0, count($data), "($numberOfQ)"));
    }

    /**
     * @param array|string $data
     * @return string
     */
    protected function returnRequiredQuestionMarks(array|string $data): string
    {
        if (is_string($data)) {
            $data = [$data];
        }

        if (!is_array(reset($data))) $data = [$data];
        # Returns question marks, e.g, if the num column of what we would be inserting is 3, it returns "?,?,?"
        return implode(',', array_fill(0, count(array_keys(reset($data))), '?'));
    }

    protected function delimitArrayByComma(array $data): string
    {
        return implode(',', $data);
    }

    /**
     * @param $update
     * @return string
     */
    protected function delimitedForInsertOnDuplicate($update): string
    {
        $getTable = $this->getTonicsQueryBuilder()->getTables();
        return implode(", ", array_map(function ($column) use ($getTable) {
            # adding the backtick one at a time, which should give us ^ "`wl_name` = values(`wl_name`),`wl_slug` = values(`wl_slug`)"
            $column = $getTable->transformTableColumn('', $column);
            return $column . ' = ' . "values($column)";
        }, $update));
    }

    /**
     * @param array $columns
     * @return string
     */
    protected function escapeDelimitedColumns(array $columns): string
    {
        $getTable = $this->getTonicsQueryBuilder()->getTables();
        return implode(",", array_map(function ($column) use ($getTable) {
            # adding the backtick one at a time
            # str_replace replaces any quote
            return $getTable->transformTableColumn('', str_replace("\"", '', $column));
        }, $columns));
    }

    /**
     * @param array $columns
     * @return string
     */
    protected function returnColumnsSeparatedByCommas(array $columns): string
    {
        return implode(",", array_map(function ($column) {
            # str_replace replaces any quote
            return str_replace("\"", '', $column);
        }, $columns));
    }

    /**
     * @throws \Exception
     */
    protected function validateNewInstanceOfTonicsQuery(TonicsQuery $tonicsQuery): void
    {
        if ($tonicsQuery === $this){
            throw new \Exception("A new instance of TonicsQuery should be passed in subQuery");
        }
    }

    /**
     * @param $condition
     * @param callable $callback
     * @param callable|null $callbackElse
     * @return static
     */
    public function when($condition, callable $callback, callable $callbackElse = null): static
    {
        if ($condition) {
            $callback($this);
        } else {
            if($callbackElse){
                $callbackElse($this);
            }
        }
        return $this;
    }

    /**
     * @param string $url
     * @return string
     */
    protected function cleanUrl(string $url): string
    {
        ## D preg_replace converts multiple slashes to one.
        ## FILTER_SANITIZE_URL remove illegal chars from the url
        ## rtrim remove slash from the end e.g /name/book/ becomes  /name/book
        return rtrim(filter_var(preg_replace("#//+#", "\\1/", $url), FILTER_SANITIZE_URL), '/');
    }

    protected function getURLParams(): array
    {
        return $this->urlParams;
    }

    /**
     * @param array $params
     * @return TonicsQuery
     */
    public function setURLParams(array $params): TonicsQuery
    {
        $this->urlParams = $params;
        return $this;
    }

    /**
     * @inheritDoc
     */
    protected function getRequestURL(): string
    {
        $url = strtok($_SERVER['REQUEST_URI'], '?');
        return $this->cleanUrl($url);
    }

    protected function getRequestURLWithQueryString(): string
    {
        $queryString = http_build_query($this->getURLParams());
        $urlPathWithoutQueryString = $this->getRequestURL();
        return $urlPathWithoutQueryString . '?' . $queryString;
    }

    /**
     * @param string $queryString
     * e.g "page=55" or "page=55&id=600" (you use the & symbol to add more query key and value)
     */
    protected function appendQueryString(string $queryString): static
    {

        $params = [];
        parse_str($queryString, $params);
        $params = [...$this->getURLParams(), ...$params];
        if(count($params) > 0) {
            $this->setURLParams($params);
        }
        return $this;
    }

    /**
     * @param $sqlResult
     * @param $page
     * @param $totalPages
     * @param $perPage
     * @param $pageName
     * @return object
     * @throws \Exception
     */
    private function arrangePagination($sqlResult, $page, $totalPages, $perPage, $pageName): object
    {
        $currentUrl = $this->getRequestURLWithQueryString();
        $numberLinks = []; $windowSize = 5;
        if ($page > 1) {
            // Number links that should appear on the left
            for($i = $page - $windowSize; $i < $page; $i++){
                if($i > 0){
                    $numberLinks[] = [
                        'link' => $this->appendQueryString("$pageName=" . $i)->getRequestURLWithQueryString(),
                        'number' => $i,
                        'current' => false,
                        'current_text' => 'Page Number ' . $i,
                    ];
                }
            }
        }
        // current page
        $numberLinks[] = [
            'link' => $this->appendQueryString("$pageName=" . $page)->getRequestURLWithQueryString(),
            'number' => $page,
            'current' => true,
            'current_text' => 'Current Page',
        ];
        // Number links that should appear on the right
        for($i = $page + 1; $i <= $totalPages; $i++){
            $numberLinks[] = [
                'link' => $this->appendQueryString("$pageName=" . $i)->getRequestURLWithQueryString(),
                'number' => $i,
                'current' => false,
                'current_text' => 'Page Number ' . $i,
            ];
            if($i >= $page + $windowSize){
                break;
            }
        }

        return (object)[
            'current_page' => (int)$page,
            'data' => $sqlResult,
            'path' => $currentUrl,
            'first_page_url' => $this->appendQueryString("$pageName=1")->getRequestURLWithQueryString(),
            'next_page_url' => ($page != $totalPages) ? $this->appendQueryString("$pageName=" . ($page + 1))->getRequestURLWithQueryString() : null,
            'prev_page_url' => ($page > 1) ? $this->appendQueryString("$pageName=" . ($page - 1))->getRequestURLWithQueryString() : null,
            'from' => 1,
            'next_page' => ($page != $totalPages) ? $page + 1: null,
            'last_page' => $totalPages,
            'last_page_url' => $this->appendQueryString("$pageName=" . $totalPages)->getRequestURLWithQueryString(),
            'per_page' => $perPage,
            'to' => $totalPages,
            'total' => $totalPages,
            'has_more' => !(((int)$page === $totalPages)),
            'number_links' => $numberLinks
        ];
    }

    /**
     * This is a standalone query and as such, the statement wouldn't be added to the SqlString
     * however, the row count would be saved in the rowCount property, so, you can check the rowCount if you like
     * @param string $statement
     * @param ...$params
     * @return array|int
     */
    public function query(string $statement, ...$params): array|int
    {
        $stmt = $this->getPdo()->prepare($statement);
        $stmt->execute($params);
        $this->setRowCount($stmt->rowCount());
        return $stmt->fetchAll($this->getPdoFetchType());
    }

    /**
     * Alias for `query()`
     * @param string $statement
     * @param ...$params
     * @return array|false
     */
    public function run(string $statement, ...$params): bool|array
    {
        $stmt = $this->getPdo()->prepare($statement);
        $stmt->execute($params);
        $this->setRowCount($stmt->rowCount());
        return $stmt->fetchAll($this->getPdoFetchType());
    }

    /**
     * @param string $statement
     * @param ...$params
     * @return mixed
     */
    public function row(string $statement, ...$params): mixed
    {
        $stmt = $this->getPdo()->prepare($statement);
        $stmt->execute($params);
        $this->setRowCount($stmt->rowCount());
        return $stmt->fetch($this->getPdoFetchType());
    }


    public function getPdo(): PDO
    {
        return $this->getTonicsQueryBuilder()->getPdo();
    }

    public function beginTransaction(): bool
    {
        return $this->getTonicsQueryBuilder()->getPdo()->beginTransaction();
    }

    public function commit(): bool
    {
        return $this->getTonicsQueryBuilder()->getPdo()->commit();
    }

    public function rollBack()
    {
        return $this->getTonicsQueryBuilder()->getPdo()->rollBack();
    }

    /**
     * @return bool|array
     */
    public function FetchResult(): bool|array
    {
        $stmt = $this->getPdo()->prepare($this->getSqlString());
        $stmt->execute($this->getParams());
        $this->setRowCount($stmt->rowCount());
        return $stmt->fetchAll($this->getPdoFetchType());
    }

    /**
     * @return mixed
     */
    public function FetchFirst(): mixed
    {
        $stmt = $this->getPdo()->prepare($this->getSqlString());
        $stmt->execute($this->getParams());
        $this->setRowCount($stmt->rowCount());
        return $stmt->fetch($this->getPdoFetchType());
    }
}