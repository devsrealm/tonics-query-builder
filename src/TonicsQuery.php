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
}