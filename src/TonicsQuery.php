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

    public function __construct(TonicsQueryBuilder $tonicsQueryBuilder = null)
    {
        if ($tonicsQueryBuilder){
            $this->tonicsQueryBuilder = $tonicsQueryBuilder;
        }
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
    public function Select(string|TonicsQuery $select): static
    {
        if ($this->isLastEmitted('SELECT')){
            if (is_object($select)){
                $this->validateNewInstanceOfTonicsQuery($select);
                $this->addSqlString("( SELECT {$select->getSqlString()} )");
                $this->params = [...$this->params, ...$select->getParams()];
            }
            throw new \Exception("Last emitted type was select, the current select arg should be a TonicsQuery Object");
        } else {
            $this->lastEmittedType = 'SELECT';
            $this->addSqlString("SELECT $select");
        }

        return $this;
    }

    /**
     * @param string $table
     * @return $this
     */
    public function From(string $table): static
    {
        $this->lastEmittedType = 'FROM';
        $this->addSqlString("FROM $table");
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
        $this->lastEmittedType = 'WHERE';
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
     * @param int $number
     * @return TonicsQuery
     */
    public function Limit(int $number): static
    {
        $this->lastEmittedType = 'LIMIT';
        $this->addSqlString("LIMIT(?)");
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
        $this->lastEmittedType = 'OFFSET';
        $this->addSqlString("OFFSET(?)");
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
     * @param string $column
     * @return $this
     */
    public function OrderBy(string $column): static
    {
        $orderBy = 'ORDER BY ';
        if($this->isLastEmitted('ORDER BY')){
            $orderBy = ', ';
        }
        $this->lastEmittedType = 'ORDER BY';
        $this->addSqlString("$orderBy$column");
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

        $this->lastEmittedType = 'ORDER BY';
        $this->addSqlString("$orderBy$column DESC");
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
    public function OrderByAsc(string $column): static
    {
        $orderBy = 'ORDER BY ';
        if($this->isLastEmitted('ORDER BY')){
            $orderBy = ', ';
        }

        $this->lastEmittedType = 'ORDER BY';
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
        $this->lastEmittedType = 'GROUP BY';
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
        $this->lastEmittedType = 'HAVING';
        $this->addSqlString("$having $first $op ?");
        $this->addParam($value);
        return $this;
    }

    /**
     * @throws \Exception
     */
    private function UnionReleated(TonicsQuery $subQuery, string $type = 'UNION'): static
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
        return $this->UnionReleated($subQuery);
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function UnionAll(TonicsQuery $subQuery): static
    {
        return $this->UnionReleated($subQuery, 'UNION ALL');
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function Intersect(TonicsQuery $subQuery): static
    {
        return $this->UnionReleated($subQuery, 'INTERSECT');
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function IntersectAll(TonicsQuery $subQuery): static
    {
        return $this->UnionReleated($subQuery, 'INTERSECT ALL');
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function Except(TonicsQuery $subQuery): static
    {
        return $this->UnionReleated($subQuery, 'EXCEPT');
    }

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function ExceptAll(TonicsQuery $subQuery): static
    {
        return $this->UnionReleated($subQuery, 'EXCEPT ALL');
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
        $this->lastEmittedType = 'WITH';
        $this->addSqlString("$with $recursiveName $cteName AS ( {$cteBody->getSqlString()} )");
        $this->addParams($cteBody->getParams());
        return $this;
    }

    private function JoinRelative(string $table, string $col, string $col2, string $op = '=', string $type = 'INNER JOIN')
    {
        $this->lastEmittedType = $type;
        $op = $this->getWhereOP($op);
        $this->addSqlString("$type $table ON $col $op $col2");
        return $this;
    }


    public function Join($table, $col, $col2, $op = '='): static
    {
        return $this->JoinRelative($table, $col, $col2, $op);
    }

    public function LeftJoin($table, $col, $col2, $op = '='): static
    {
        return $this->JoinRelative($table, $col, $col2, $op, 'LEFT JOIN');
    }

    public function RightJoin($table, $col, $col2, $op = '='): static
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

    /**
     * @param TonicsQuery $subQuery
     * @return $this
     * @throws \Exception
     */
    public function SubQuery(TonicsQuery $subQuery): static
    {
        $this->lastEmittedType = 'SubQuery';
        $this->validateNewInstanceOfTonicsQuery($subQuery);
        $this->addSqlString("( {$subQuery->getSqlString()} )");
        $this->addParams($subQuery->getParams());
        return $this;
    }

    /**
     * @param string $jsonDoc
     * @param string $path
     * @return $this
     */
    public function JsonExtract(string $jsonDoc, string $path): static
    {
        $this->lastEmittedType = 'JSON_EXTRACT';
        $this->addSqlString("JSON_EXTRACT($jsonDoc, ?)");
        $this->addParam($path);
        return $this;
    }

    /**
     * @param string $jsonDoc
     * @param ...$path
     * @return $this
     */
    public function JsonSet(string $jsonDoc, ...$path): static
    {
        $this->lastEmittedType = 'JSON_SET';
        $mark = $this->returnRequiredQuestionMarks($path);
        $this->addSqlString("JSON_SET($jsonDoc, $mark)");
        $this->addParams($path);
        return $this;
    }

    /**
     * @param string $jsonDoc
     * @param string $path
     * @return $this
     */
    public function JsonExist(string $jsonDoc, string $path): static
    {
        $this->lastEmittedType = 'JSON_EXIST';
        $this->addSqlString("JSON_EXIST($jsonDoc, ?)");
        $this->addParam($path);
        return $this;
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
    public function insert(string $table, array $data, int $chunkInsertRate = 1000): bool
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
    public function insertOnDuplicate(string $table, array $data, array $update, int $chunkInsertRate = 1000): bool
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
     * Update statement, could be followed by the `Set()` method
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
        if (!array_is_list($updateChanges)){
            throw new \Exception("Update changes should be an array list");
        }
        $updateString = "UPDATE $table SET";
        $params = [];

        $pre = [];
        foreach ($updateChanges as $col => $v) {
            if ($v === null) {
                $pre []= " {$col} = NULL";
            } elseif (is_bool($v)) {
                if ($v === true) {
                    $pre [] = " {$col} = TRUE ";
                }else {
                    $pre [] = " {$col} = FALSE ";
                }
            } else {
                $pre [] = " {$col} = ?";
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
     * For RAW SQL;
     *
     * <br>
     * Note: You should add the question mark if there is any, and use the addParam or addParams to add the paramter
     * @param string $raw
     * @return $this
     */
    public function Raw(string $raw): static
    {
        $this->addSqlString($raw);
        return $this;
    }

    /**
     * Get a new instance of TonicsQuery
     * @return TonicsQuery
     */
    public function Q()
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
    public function if($condition, callable $callback, callable $callbackElse = null): static
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

    public function getPdo(): PDO
    {
        return $this->getTonicsQueryBuilder()->getPdo();
    }

    public function GetResult(): bool|array
    {
        $stmt = $this->getPdo()->prepare($this->getSqlString());
        $stmt->execute($this->getParams());
        $this->setRowCount($stmt->rowCount());
        return $stmt->fetchAll($this->getPdoFetchType());
    }

    public function GetFirst()
    {
        $stmt = $this->getPdo()->prepare($this->getSqlString());
        $stmt->execute($this->getParams());
        $this->setRowCount($stmt->rowCount());
        return $stmt->fetch($this->getPdoFetchType());
    }
}