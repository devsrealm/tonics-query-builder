# Tonics Query Builder

A small, hookable SQL query builder with pluggable transformers for different databases. This README shows accurate usage based on the Postgres transformer spec and tests.

## Installation

Install via Composer:

```bash
composer require devsrealm/tonics-query-builder
```

## Requirements

- PHP 7.4+
- PDO extension
- A PDO driver for your database (pgsql for PostgreSQL, mysql for MySQL/MariaDB)

## Quick Start (PostgreSQL)

```php
use Devsrealm\TonicsQueryBuilder\TonicsQueryBuilder;
use Devsrealm\TonicsQueryBuilder\Transformers\Postgres\PostgresTonicsQueryTransformer;
use Devsrealm\TonicsQueryBuilder\Transformers\Postgres\PostgresTables;

$pdo = new PDO('pgsql:host=localhost;port=5432;dbname=yourdb', 'user', 'pass', [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

$tables = (new PostgresTables())
    ->addTable('users', ['id','username','email','created_at','logins'])
    ->addTable('posts', ['id','user_id','title','content','metadata','created_at']);

$qb = new TonicsQueryBuilder($pdo, new PostgresTonicsQueryTransformer(), $tables);

// Build and execute a SELECT with date filter, LIKE, order, limit/offset
$q = $qb->getNewQuery();
$users = $q->Select($tables->pickTable('users', ['id','username']))
    ->From($tables->getTable('users'))
    ->WhereDate('created_at', '>=', '2025-01-01')
    ->WhereLike('username', 'ali')
    ->OrderByAsc('"username"')
    ->Take(10)
    ->Skip(0)
    ->FetchResult();
```

### Date/Time

```php
// CAST for date/time filters and TO_CHAR for formatting
$q = $qb->getNewQuery();
$q->Select()->DateFormat('created_at', '%Y-%m-%d %H:%i:%s')->As('formatted_date')
    ->From('users')
    ->WhereTime('created_at', '>=', '10:00:00');

$row = $q->FetchFirst();
```

### String pattern matching

```php
$q = $qb->getNewQuery();
$q->Select('username')
    ->From('users')
    ->WhereLike('username', 'john')   // username LIKE '%john%'
    ->WhereStarts('email', 'admin')   // email LIKE 'admin%'
    ->WhereEnds('username', '123');   // username LIKE '%123'

$rows = $q->FetchResult();
```

### JSON helpers (jsonb)

```php
// Extract JSON field
$q = $qb->getNewQuery();
$q->Select('title')
  ->Select($q->Q()->JsonExtract('metadata', 'author'))->As('author')
  ->From('posts')
  ->Where('title', '=', 'First Post');
$first = $q->FetchFirst();

// Nested path and containment filter
$q = $qb->getNewQuery();
$posts = $q->Select('title')
    ->From('posts')
    ->WhereJsonContains('metadata', 'tags', '["featured"]', '$.')
    ->OrderByAsc('"title"')
    ->FetchResult();

// Update/merge/remove JSON
$q = $qb->getNewQuery();
$q->Update('posts')
  ->Set('metadata', $q->Q()->JsonSet('metadata', 'author', '"Updated Author"'))
  ->Where('title', '=', 'First Post')
  ->Exec();

$q = $qb->getNewQuery();
$q->Select()->JsonMergePatch('metadata', '{"new_field": "value"}');
$sql = $q->getSqlString();
```

### Upsert (ON CONFLICT)

```php
$q = $qb->getNewQuery();
$data = [
  ['username' => 'alice', 'email' => 'alice@example.com', 'logins' => 1],
  ['username' => 'bob',   'email' => 'bob@example.com',   'logins' => 1],
];

// Specify conflict columns
$q->InsertOnDuplicate('users', $data, [
  'conflict' => ['username'],
  'set' => ['email', 'logins']
]);

// Or, specify a constraint name
$q->InsertOnDuplicate('users', $data, [
  'constraint' => 'users_pkey',
  'set' => ['email', 'logins']
]);

// Batch large datasets with chunk size (e.g., 50)
$q->InsertOnDuplicate('users', $data, [
  'conflict' => ['username'],
  'set' => ['email']
], 50);
```

### Null-safe equals

```php
// MySQL `<=>` is transformed to Postgres `IS NOT DISTINCT FROM`
$q = $qb->getNewQuery();
$withNullEmails = $q->Select('username')
  ->From('users')
  ->Where('email', '<=>', null)
  ->OrderByAsc('"username"')
  ->FetchResult();
```

### Raw queries with PostgreSQL-style placeholders

If you need to execute raw SQL with PostgreSQL's native `$1, $2, $3` placeholder syntax (instead of PDO's `?`), use the `runPg()` or `rowPg()` methods:

```php
// Execute with multiple results
$q = $qb->getNewQuery();
$result = $q->runPg(
    "SELECT EXISTS(SELECT 1 FROM migrations WHERE migration = $1) AS result",
    $migrationName
);

// Execute and get single row
$q = $qb->getNewQuery();
$user = $q->rowPg(
    "SELECT * FROM users WHERE id = $1 AND status = $2",
    123,
    'active'
);
```

These methods automatically convert PostgreSQL-style placeholders to PDO format before execution.

## Key differences from MySQL/MariaDB

- Identifier quoting: PostgreSQL uses double quotes (")
- Date filtering: CAST(col AS DATE) instead of DATE(col)
- Time filtering: CAST(col AS TIME)
- Date formatting: TO_CHAR() (MySQL tokens are converted by the transformer)
- LIKE concatenation: uses `||` instead of CONCAT()
- JSON: uses jsonb operators (`#>>`, `@>`, `||`, jsonb_set)
- Upsert: uses `ON CONFLICT ... DO UPDATE`
- Null-safe equals: `IS NOT DISTINCT FROM`

## Quick Start (MySQL)

```php
use Devsrealm\TonicsQueryBuilder\TonicsQueryBuilder;
use Devsrealm\TonicsQueryBuilder\Transformers\MySQL\MySQLTonicsQueryTransformer;
use Devsrealm\TonicsQueryBuilder\Transformers\MariaDB\MariaDBTables; // reuse backtick-quoting tables

$pdo = new PDO('mysql:host=localhost;dbname=yourdb;charset=utf8mb4', 'user', 'pass', [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

$tables = (new MariaDBTables())
    ->addTable('users', ['id','username','email','created_at']);

$qb = new TonicsQueryBuilder($pdo, new MySQLTonicsQueryTransformer(), $tables);

// Example: Insert and return inserted rows (emulated)
$q = $qb->getNewQuery();
$data = [
    ['username' => 'alice', 'email' => 'alice@example.com'],
    ['username' => 'bob',   'email' => 'bob@example.com'],
];
$inserted = $q->InsertReturning('users', $data, ['id','username','email'], 'id');

// Example: Basic SELECT
$q = $qb->getNewQuery();
$users = $q->Select($tables->pickTable('users', ['id','username']))
    ->From($tables->getTable('users'))
    ->Where('username', 'LIKE', '%ali%')
    ->OrderByAsc('`username`')
    ->Take(10)
    ->FetchResult();
```

## Quick Start (MariaDB)

```php
use Devsrealm\TonicsQueryBuilder\TonicsQueryBuilder;
use Devsrealm\TonicsQueryBuilder\Transformers\MariaDB\MariaDBTonicsQueryTransformer;
use Devsrealm\TonicsQueryBuilder\Transformers\MariaDB\MariaDBTables;

$pdo = new PDO('mysql:host=localhost;dbname=yourdb;charset=utf8mb4', 'user', 'pass', [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

$tables = (new MariaDBTables())
    ->addTable('users', ['id','username','email','created_at']);

$qb = new TonicsQueryBuilder($pdo, new MariaDBTonicsQueryTransformer(), $tables);

// Example: Basic SELECT
$q = $qb->getNewQuery();
$users = $q->Select($tables->pickTable('users', ['id','username','email']))
    ->From($tables->getTable('users'))
    ->Where('created_at', '>=', '2025-01-01')
    ->OrderByAsc('`username`')
    ->Take(10)
    ->FetchResult();
```

## Testing

This repo ships with Kahlan specs. To run tests (requires a local PostgreSQL and PHP):

- Windows (cmd.exe):

```bat
vendor\bin\kahlan.bat
```

- Linux/macOS:

```bash
vendor/bin/kahlan
```

See `TESTING.md` for environment variables and setup details.

## License

MIT
