<?php

namespace Devsrealm\TonicsQueryBuilder\Transformers\Postgres;

use Devsrealm\TonicsQueryBuilder\TonicsQuery;

/**
 * PostgreSQL Transformer for Tonics Query Builder
 *
 * Adapts the builder's MySQL/MariaDB-oriented API to PostgreSQL syntax.
 *
 * Quick usage (mirrors spec tests):
 *
 * ```
 *   $pdo = new \PDO('pgsql:host=localhost;port=5432;dbname=yourdb', 'user', 'pass', [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION]);
 *   $tables = (new PostgresTables())
 *       ->addTable('users', ['id','username','email','created_at','logins'])
 *       ->addTable('posts', ['id','user_id','title','content','metadata','created_at']);
 *   $qb = new \Devsrealm\TonicsQueryBuilder\TonicsQueryBuilder($pdo, new PostgresTonicsQueryTransformer(), $tables);
 *
 *   // SELECT with date filter, LIKE, order, pagination
 *   $q = $qb->getNewQuery();
 *   $users = $q->Select($tables->pickTable('users', ['id','username']))
 *       ->From($tables->getTable('users'))
 *       ->WhereDate('created_at', '>=', '2025-01-01')
 *       ->WhereLike('username', 'ali')
 *       ->OrderByAsc('"username"')
 *       ->Take(10)
 *       ->Skip(0)
 *       ->FetchResult();
 *
 *   // Format dates (MySQL tokens auto-converted to Postgres TO_CHAR)
 *   $q = $qb->getNewQuery();
 *   $row = $q->Select()->DateFormat('created_at', '%Y-%m-%d %H:%i:%s')->As('formatted')
 *       ->From('users')
 *       ->Where('username', '=', 'alice')
 *       ->FetchFirst();
 *
 *   // JSON extract and JSON containment filter
 *   $q = $qb->getNewQuery();
 *   $posts = $q->Select('title')
 *       ->Select($q->Q()->JsonExtract('metadata', 'author'))->As('author')
 *       ->From('posts')
 *       ->WhereJsonContains('metadata', 'tags', '["featured"]', '$.')
 *       ->OrderByAsc('"title"')
 *       ->FetchResult();
 *
 *   // Update JSON using jsonb_set
 *   $q = $qb->getNewQuery();
 *   $q->Update('posts')
 *       ->Set('metadata', $q->Q()->JsonSet('metadata', 'author', '\"Updated Author\"'))
 *       ->Where('title', '=', 'First Post')
 *       ->Exec();
 *
 *   // Upsert (ON CONFLICT) — specify conflict columns or constraint
 *   $q = $qb->getNewQuery();
 *   $data = [
 *       ['username' => 'alice', 'email' => 'alice@example.com', 'logins' => 1],
 *       ['username' => 'bob',   'email' => 'bob@example.com',   'logins' => 1],
 *   ];
 *   $q->InsertOnDuplicate('users', $data, [
 *       'conflict' => ['username'],
 *       'set' => ['email', 'logins']
 *   ]);
 *   // or: $q->InsertOnDuplicate('users', $data, ['constraint' => 'users_pkey', 'set' => ['email','logins']]);
 *
 *   // Null-safe equals: MySQL `<=>` becomes Postgres `IS NOT DISTINCT FROM`
 *   $q = $qb->getNewQuery();
 *   $nullMatches = $q->Select('username')
 *       ->From('users')
 *       ->Where('email', '<=>', null)
 *       ->OrderByAsc('"username"')
 *       ->FetchResult();
 * ```
 */
class PostgresTonicsQueryTransformer extends TonicsQuery
{
    /*
     |--------------------------------------------------------------------------
     | Date/Time helpers
     |--------------------------------------------------------------------------
     */
    public function WhereDate(string $col, string $op, string $value): static
    {
        $op = $this->getWhereOP($op);
        $this->addSqlString("{$this->getWhere()} CAST($col AS DATE) $op ?");
        $this->addParam($value);
        return $this;
    }

    public function WhereTime(string $col, string $op, string $value): static
    {
        $op = $this->getWhereOP($op);
        $this->addSqlString("{$this->getWhere()} CAST($col AS TIME) $op ?");
        $this->addParam($value);
        return $this;
    }

    public function DateFormat(string $date, string $format = '%Y-%m-%d %H:%i:%s'): static
    {
        $pgFormat = $this->convertMySQLDateFormatToPostgres($format);
        // $date is a column name, not a parameter - use it directly in SQL
        $this->addSqlString("TO_CHAR($date, ?)");
        $this->addParam($pgFormat);
        return $this;
    }

    public function WhereDateFormat(string $date, string $format = '%Y-%m-%d %H:%i:%s'): static
    {
        $pgFormat = $this->convertMySQLDateFormatToPostgres($format);
        // $date is a column name, not a parameter - use it directly in SQL
        $this->addSqlString("{$this->getWhere()} TO_CHAR($date, ?)");
        $this->addParam($pgFormat);
        return $this;
    }

    private function convertMySQLDateFormatToPostgres(string $format): string
    {
        $map = [
            '%Y' => 'YYYY',
            '%y' => 'YY',
            '%m' => 'MM',
            '%c' => 'MM',
            '%d' => 'DD',
            '%e' => 'DD',
            '%H' => 'HH24',
            '%h' => 'HH12',
            '%I' => 'HH12',
            '%i' => 'MI',
            '%s' => 'SS',
            '%S' => 'SS',
            '%M' => 'Mon',
            '%b' => 'Mon',
        ];
        return strtr($format, $map);
    }

    /*
     |--------------------------------------------------------------------------
     | LIKE helpers (use concatenation operator ||)
     |--------------------------------------------------------------------------
     */
    public function WhereEnds(string $col, string $value): static
    {
        $this->addSqlString("{$this->getWhere()} $col LIKE (? || ?)");
        $this->addParams(['%', $value]);
        return $this;
    }

    public function WhereStarts(string $col, string $value): static
    {
        $this->addSqlString("{$this->getWhere()} $col LIKE (? || ?)");
        $this->addParams([$value, '%']);
        return $this;
    }

    public function WhereLike(string $col, string $value): static
    {
        $this->addSqlString("{$this->getWhere()} $col LIKE (? || ? || ?)");
        $this->addParams(['%', $value, '%']);
        return $this;
    }

    /*
     |--------------------------------------------------------------------------
     | JSON helpers (jsonb)
     |--------------------------------------------------------------------------
     */

    public function JsonExtract(string $jsonDoc, string $path, string $accessor = '$.'): static
    {
        $this->setLastEmittedType('JSON_EXTRACT');
        $pgPath = $this->mysqlJsonPathToPgArray($accessor . $path);
        // text value extraction
        $this->addSqlString("($jsonDoc)::jsonb #>> (?::text[])");
        $this->addParam($pgPath);
        return $this;
    }

    /**
     * @throws \Exception
     */
    public function JsonSet(string $jsonDoc, ...$params): static
    {
        $this->setLastEmittedType('JSON_SET');
        if (empty($params) || (count($params) % 2 !== 0)) {
            throw new \InvalidArgumentException('JsonSet requires path/value pairs');
        }

        $expr = "($jsonDoc)::jsonb";
        for ($i = 0; $i < count($params); $i += 2) {
            $path = $params[$i];
            $value = $params[$i + 1];

            $pgPath = $this->mysqlJsonPathToPgArray(is_string($path) ? $path : (string)$path);
            $template = "jsonb_set($expr, ?::text[], %s, true)";
            // Bind the path param first
            $this->addParam($pgPath);

            if ($value instanceof TonicsQuery) {
                $this->validateNewInstanceOfTonicsQuery($value);
                $valueSQL = "(" . $value->getSqlString() . ")";
                $expr = sprintf($template, $valueSQL);
                $this->addParams($value->getParams());
                $this->checkAndCloseTonicsQueryPassedToParam($value);
            } else {
                $expr = sprintf($template, "(?::jsonb)");
                $this->addParam($value);
            }
        }

        $this->addSqlString($expr);
        return $this;
    }

    public function JsonRemove(string $jsonDoc, ...$path): static
    {
        $this->setLastEmittedType('JSON_REMOVE');
        if (empty($path)) {
            return $this;
        }
        $expr = "($jsonDoc)::jsonb";
        foreach ($path as $p) {
            $pgPath = $this->mysqlJsonPathToPgArray(is_string($p) ? $p : (string)$p);
            $expr = "$expr #- (?::text[])";
            $this->addParam($pgPath);
        }
        $this->addSqlString($expr);
        return $this;
    }

    public function JsonUnquote(string $value): static
    {
        // In PG, if value is JSON string, casting to jsonb then ->>'' is awkward; for a param, just return it as text
        $this->setLastEmittedType('JSON_UNQUOTE');
        $this->addSqlString("(?::text)");
        $this->addParam($value);
        return $this;
    }

    public function JsonCompact(string $value): static
    {
        // Hint that provided value is a full JSON payload
        $this->setLastEmittedType('JSON_COMPACT');
        $this->addSqlString("(?::jsonb)");
        $this->addParam($value);
        return $this;
    }

    public function JsonExist(string $jsonDoc, string $path, string $accessor = '$.'): static
    {
        $this->setLastEmittedType('JSON_EXIST');
        $pgPath = $this->mysqlJsonPathToPgArray($accessor . $path);
        // Existence checks: jsonb_path_exists can use a JSONPath, but we approximate with path extraction not null
        $this->addSqlString("(($jsonDoc)::jsonb #> (?::text[])) IS NOT NULL");
        $this->addParam($pgPath);
        return $this;
    }

    public function JsonContain(string $jsonDoc, string $path, string $value, string $accessor = '$.'): static
    {
        $this->setLastEmittedType('JSON_CONTAINS');
        // Only support root path for a reliable implementation; else advise using a manual expression
        $pgPath = $this->mysqlJsonPathToPgArray($accessor . $path);
        $this->addSqlString("(($jsonDoc)::jsonb #> (?::text[])) @> (?::jsonb)");
        $this->addParam($pgPath);
        $this->addParam($value);
        return $this;
    }

    public function WhereJsonContains(string $jsonDoc, string $path, string $value, string $accessor = '$.', string $ifWhereUse = 'AND'): static
    {
        $this->setLastEmittedType('JSON_CONTAINS');
        // Don't set 'WHERE' before calling getWhere() - it will set it internally
        $pgPath = $this->mysqlJsonPathToPgArray($accessor . $path);
        $this->addSqlString("{$this->getWhere($ifWhereUse)} (($jsonDoc)::jsonb #> (?::text[])) @> (?::jsonb)");
        $this->addParam($pgPath);
        $this->addParam($value);
        return $this;
    }

    public function JsonMergePatch(string $jsonDoc, string $jsonDoc2): static
    {
        $this->setLastEmittedType('JSON_MERGE_PATCH');
        // In PG, jsonb concatenation merges objects (last wins)
        $this->addSqlString("(($jsonDoc)::jsonb || ($jsonDoc2)::jsonb)");
        return $this;
    }

    public function JsonArrayAppend(string $jsonDoc, array $data)
    {
        // Non-trivial in PG; prefer explicit SQL. Provide a helpful error.
        throw new \RuntimeException('JsonArrayAppend is not implemented for Postgres transformer; build explicit jsonb_set/jsonb_insert manually.');
    }

    /*
     |--------------------------------------------------------------------------
     | Upsert differences
     |--------------------------------------------------------------------------
     */
    public function InsertOnDuplicate(string $table, array $data, array $update, int $chunkInsertRate = 1000): bool
    {
        if (empty($data)) return false;
        if (!is_array(reset($data))) $data = [$data];

        // Determine columns to SET and the conflict target
        $setColumns = [];
        $conflictColumns = null;     // array|null
        $conflictConstraint = null;  // string|null

        $isAssoc = array_keys($update) !== range(0, count($update) - 1);
        if ($isAssoc) {
            if (isset($update['set'])) {
                $setColumns = $update['set'];
            } elseif (isset($update['columns'])) {
                $setColumns = $update['columns'];
            }
            if (isset($update['conflict'])) {
                $conflictColumns = $update['conflict'];
                if (!is_array($conflictColumns)) $conflictColumns = [$conflictColumns];
            }
            if (isset($update['constraint'])) {
                $conflictConstraint = $update['constraint'];
            }
            if (empty($setColumns)) {
                // Fallback: if the user passed an assoc array of columns without explicit key
                $setColumns = array_values(array_diff(array_keys($update), ['conflict', 'constraint', 'set', 'columns']));
            }
        } else {
            $setColumns = $update;
        }

        if (empty($setColumns)) {
            throw new \InvalidArgumentException('InsertOnDuplicate requires columns to SET. Use ["set" => [...]] or a simple list.');
        }

        // If no explicit conflict target, attempt to infer 'id' as a common PK
        if ($conflictColumns === null && $conflictConstraint === null) {
            $firstRow = reset($data);
            if (is_array($firstRow) && array_key_exists('id', $firstRow)) {
                $conflictColumns = ['id'];
            } else {
                throw new \InvalidArgumentException('Postgres InsertOnDuplicate requires a conflict target: pass ["conflict" => [..]] or ["constraint" => "..."]; or include an "id" column to infer.');
            }
        }

        // Build column list for INSERT
        $getColumns = array_keys(reset($data));
        $delimitedColumns = $this->escapeDelimitedColumns($getColumns);

        // Build SET list: col = EXCLUDED.col
        $tables = $this->getTonicsQueryBuilder()->getTables();
        $setList = implode(', ', array_map(function ($column) use ($tables) {
            $quoted = $tables->transformTableColumn('', $column);
            return $quoted . ' = EXCLUDED.' . $quoted;
        }, $setColumns));

        // Build ON CONFLICT clause
        if ($conflictConstraint) {
            $conflictSQL = "ON CONFLICT ON CONSTRAINT $conflictConstraint";
        } else {
            $quotedCols = implode(',', array_map(fn($c) => $tables->transformTableColumn('', $c), $conflictColumns));
            $conflictSQL = "ON CONFLICT ($quotedCols)";
        }

        $rowCount = 0;
        foreach (array_chunk($data, $chunkInsertRate) as $toInsert) {
            $valuesMarks = $this->returnRequiredQuestionMarksSurroundedWithParenthesis($toInsert);
            $flattened = iterator_to_array(new \RecursiveIteratorIterator(new \RecursiveArrayIterator($toInsert)), 0);
            $sql = "INSERT INTO $table ($delimitedColumns) VALUES $valuesMarks $conflictSQL DO UPDATE SET $setList";
            $stmt = $this->getPdo()->prepare($sql);
            $stmt->execute($flattened);
            $rowCount += $stmt->rowCount();
        }

        $this->setRowCount($rowCount);
        return $this->getRowCount() > 0;
    }

    /*
     |--------------------------------------------------------------------------
     | Operators
     |--------------------------------------------------------------------------
     */
    public function NullSafeEqualOperator(): string
    {
        // Postgres equivalent to MySQL's <=>
        return 'IS NOT DISTINCT FROM';
    }

    /*
     |--------------------------------------------------------------------------
     | Helpers
     |--------------------------------------------------------------------------
     */
    private function mysqlJsonPathToPgArray(string $path): string
    {
        // Expect paths like '$.a.b' or 'a.b'. Convert to '{a,b}' string for text[] cast
        $path = ltrim($path, '$.');
        $segments = array_values(array_filter(explode('.', $path), fn($v) => $v !== ''));
        return '{' . implode(',', $segments) . '}';
    }
}
