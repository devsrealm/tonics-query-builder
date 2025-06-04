<?php

namespace Devsrealm\TonicsQueryBuilder\Transformers\MySQL;

use Devsrealm\TonicsQueryBuilder\TonicsQuery;

class MySQLTonicsQueryTransformer extends TonicsQuery
{
    public function InsertReturning(string $table, array $data, array $return, string $primaryKey): mixed
    {
        if (empty($data)) return false;

        if (!is_array(reset($data))) $data = [$data];

        if (!is_array(reset($return))) $return = [$return];

        #
        # VALIDATION AND DELIMITATION FOR THE ACTUAL COLUMN
        #
        $getColumns = array_keys(reset($data));
        # e.g, "`column1`,`column1`,`column1`",
        $delimitedColumns = $this->escapeDelimitedColumns($getColumns);
        $numberOfQ = $this->returnRequiredQuestionMarksSurroundedWithParenthesis($data);

        #
        # VALIDATION AND DELIMITATION FOR RETURNING
        #
        $getReturningColumns = (array)array_values(reset($return));
        $delimitedReturningColumns = $this->escapeDelimitedColumns($getReturningColumns);

        $pdo = $this->getTonicsQueryBuilder()->getPdo();

        try {
            $pdo->beginTransaction();

            #
            # SQL PREPARE, INSERTION
            #
            $sql = "INSERT INTO $table ($delimitedColumns) VALUES $numberOfQ";
            $stmt = $pdo->prepare($sql);
            $flattened = iterator_to_array(new \RecursiveIteratorIterator(new \RecursiveArrayIterator($data)), 0);
            $stmt->execute($flattened);
            $rowCount = $stmt->rowCount();
            $lastInsertID = $pdo->lastInsertId();

            // Handle case where lastInsertId() returns 0 or null (non-auto-increment tables)
            if (empty($lastInsertID)) {
                // For non-auto-increment primary keys, we need a different approach
                // Try to get the inserted records by matching the exact data
                $whereConditions = [];
                $whereParams = [];

                foreach ($data as $row) {
                    $rowConditions = [];
                    foreach ($row as $column => $value) {
                        $escapedColumn = $this->escapeColumn($column);
                        $rowConditions[] = "$escapedColumn = ?";
                        $whereParams[] = $value;
                    }
                    $whereConditions[] = '(' . implode(' AND ', $rowConditions) . ')';
                }

                $whereClause = implode(' OR ', $whereConditions);
                $sqlReturningAlternative = "SELECT $delimitedReturningColumns FROM $table WHERE $whereClause";

                $stmtReturning = $pdo->prepare($sqlReturningAlternative);
                $stmtReturning->execute($whereParams);
            } else {
                ### DATA RETURNING - Fixed MySQL syntax
                $sqlReturningAlternative = "SELECT $delimitedReturningColumns FROM $table WHERE $primaryKey >= ? ORDER BY $primaryKey LIMIT ?";

                $stmtReturning = $pdo->prepare($sqlReturningAlternative);
                $stmtReturning->execute([$lastInsertID, $rowCount]);
            }

            // For single row insert, return single record; for multiple rows, return array
            if (count($data) === 1) {
                $result = $stmtReturning->fetch($this->getPdoFetchType());
            } else {
                $result = $stmtReturning->fetchAll($this->getPdoFetchType());
            }

            $pdo->commit();
            $this->setRowCount($rowCount);
            return $result;

        } catch (\Exception $exception) {
            $pdo->rollBack();
            throw $exception; // Re-throw to help with debugging
        }
    }

    /**
     * Helper method to escape a single column name
     */
    private function escapeColumn(string $column): string
    {
        return '`' . str_replace('`', '``', $column) . '`';
    }
}