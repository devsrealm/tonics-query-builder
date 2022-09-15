<?php

namespace Devsrealm\TonicsQueryBuilder;

abstract class Tables
{
    private array $tables = [];
    private string $tablePrefix;

    public function __construct(string $tablePrefix = '')
    {
        $this->tablePrefix = $tablePrefix;
    }

    /**
     * @param string $tableName
     * @param array $column
     * @return $this
     */
    public function addTable(string $tableName, array $column): static
    {
        $this->tables[$tableName] = array_combine($column, $column);
        return $this;
    }

    /**
     * @return string
     */
    public function getAllColumn(): string
    {
        $colString = '';
        $tables = $this->tables;
        foreach ($tables as $table => $columns){
            $tablePrefixed = $this->getTable($table);
            foreach ($columns as $column){
                $colString .= "{$this->transformTableColumn($tablePrefixed, $column)}, ";
            }
        }

        return rtrim($colString, ', ');
    }

    /**
     * Pick a column from table,
     * should be in the format
     *
     * ```
     * [ 'table_name' => ['col_1', 'col_2'], 'table_name_2' => ['col_1', 'col_2'],]
     * ```
     *
     * <br>
     * You'll get the following result depending on the transformer:
     *
     * ```
     * "prefix.table_name.col_1, prefix.table_name.col_2, prefix.table_name_2.col_1, prefix.table_name_2.col_2"
     * ```
     * @param array $tableToColumns
     * @return string
     * @throws \Exception
     */
    public function pick(array $tableToColumns): string
    {
        $colString = '';
        foreach ($tableToColumns as $table => $columnToPick){
            $getColumns = $this->getTableColumnsAsArray($table);
            $tablePrefixed = $this->getTable($table);
            if (!is_array($columnToPick)){
                throw new \Exception("Column to pick should be an array");
            }
            foreach ($columnToPick as $col){
                if (key_exists($col, $getColumns)){
                    $colString .= "{$this->transformTableColumn($tablePrefixed, $col)}, ";
                }
            }
        }

        return rtrim($colString, ', ');
    }

    /**
     * @param $table
     * @param array $column
     * @return string
     * @throws \Exception
     */
    public function pickTable($table, array $column): string
    {
        return $this->pick([$table => $column]);
    }

    /**
     * @param string $tableName
     * @param string $col
     * @return string
     * @throws \Exception
     */
    public function getColumn(string $tableName, string $col)
    {
        return $this->pick([ $tableName => [$col]]);
    }

    /**
     * Pick all column from tables except the following specified in the following format:
     *
     * ```
     * [ 'table_name' => ['col_1', 'col_2'], 'table_name_2' => ['col_1', 'col_2'],]
     * ```
     * @param array $tableToColumns
     * @return string
     * @throws \Exception
     */
    public function except(array $tableToColumns): string
    {
        $colString = '';
        foreach ($tableToColumns as $table => $columnToPick){
            $getColumns = $this->getTableColumnsAsArray($table);
            $tablePrefixed = $this->getTable($table);
            if (!is_array($columnToPick)){
                throw new \Exception("Column to pick should be an array");
            }

            $columnToPick = array_combine($columnToPick, $columnToPick);
            foreach ($getColumns as $col){
                if (key_exists($col, $columnToPick)){
                    continue;
                }
                $colString .= "{$this->transformTableColumn($tablePrefixed, $col)}, ";
            }
        }

        return rtrim($colString, ', ');
    }

    /**
     * @param string $tableName
     * @return array
     */
    private function getTableColumnsAsArray(string $tableName): array
    {
        if ($this->isTable($tableName)){
            return $this->tables[$tableName];
        }
        return [];

    }

    /**
     * @param string $tableName
     * @return string
     */
    private function getTableColumnsAsString(string $tableName): string
    {
        $tablePrefixed = $this->getTable($tableName);
        $columns = $this->tables[$tableName];

        $colString = '';
        foreach ($columns as $column){
            $colString .= "{$this->transformTableColumn($tablePrefixed, $column)}, ";
        }

        return rtrim($colString, ', ');
    }

    public function getTable(string $tablename): string
    {
        if ($this->isTable($tablename)){
            return $this->getTablePrefix() . $tablename;
        }
        throw new \InvalidArgumentException("`$tablename` is an invalid table name");
    }

    /**
     * @param string $tablename
     * @return bool
     */
    public function isTable(string $tablename): bool
    {
        return isset($this->tables[$tablename]);
    }

    /**
     * @param string $table
     * @param $col
     * @return bool
     */
    public function hasColumn(string $table, $col): bool
    {
        if ($this->isTable($table)){
            $column = $this->tables[$table];
            return key_exists($col, $column);
        }

        return false;
    }

    /**
     * @inheritDoc
     */
    public function transformTableColumn($table, $col): string
    {
        if (empty($table)){
            return "`$col`";
        }
        return "$table.`$col`";
    }
    
    /**
     * @return array
     */
    public function getTables(): array
    {
        return $this->tables;
    }

    /**
     * @param array $tables
     * @return Tables
     */
    public function setTables(array $tables): Tables
    {
        $this->tables = $tables;
        return $this;
    }

    /**
     * @return string
     */
    public function getTablePrefix(): string
    {
        return $this->tablePrefix;
    }

    /**
     * @param string $tablePrefix
     * @return Tables
     */
    public function setTablePrefix(string $tablePrefix): Tables
    {
        $this->tablePrefix = $tablePrefix;
        return $this;
    }
}