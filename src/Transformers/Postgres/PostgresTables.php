<?php

namespace Devsrealm\TonicsQueryBuilder\Transformers\Postgres;

use Devsrealm\TonicsQueryBuilder\Tables;

class PostgresTables extends Tables
{
    public function transformTableColumn($table, $col): string
    {
        $q = fn(string $ident) => '"' . str_replace('"', '""', $ident) . '"';

        if (!empty($table)) {
            if (str_contains($table, '.')) {
                $parts = array_filter(explode('.', $table), 'strlen');
                $quotedParts = array_map($q, $parts);
                $quotedTable = implode('.', $quotedParts);
            } else {
                $quotedTable = $q($table);
            }
            return $quotedTable . '.' . $q($col);
        }

        return $q($col);
    }
}
