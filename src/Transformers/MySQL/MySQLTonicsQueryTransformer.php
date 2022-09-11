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

            ### DATA RETURNING
            $sqlReturningAlternative = <<<SQL
SELECT $delimitedReturningColumns FROM $table
WHERE $primaryKey >= $lastInsertID ORDER BY $primaryKey FETCH FIRST $rowCount ROWS ONLY;
SQL;

            $stmtReturning = $pdo->prepare($sqlReturningAlternative);
            $stmtReturning->execute();
            $pdo->commit();

            $this->setRowCount($rowCount);
            return $stmtReturning->fetchAll($this->getPdoFetchType());
        } catch (\Exception $exception){
            $pdo->rollBack();
        }

        return [];
    }
}