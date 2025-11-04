<?php

use Devsrealm\TonicsQueryBuilder\TonicsQueryBuilder;
use Devsrealm\TonicsQueryBuilder\Transformers\Postgres\PostgresTonicsQueryTransformer;
use Devsrealm\TonicsQueryBuilder\Transformers\Postgres\PostgresTables;

/**
 * @property TonicsQueryBuilder $qb
 * @property PostgresTonicsQueryTransformer $query
 * @property PostgresTables $tables
 * @property \PDO $pdo
 * @property \PDO $masterPdo
 * @property string $templateDb
 * @property string $testDb
 * @property bool $postgresAvailable
 * @property array $connectionParams
 * @property callable $loadEnv
 */
describe('PostgresTonicsQueryTransformer', function() {

    beforeAll(function() {
        // Load environment variables from .env.test or .env
        $loadEnv = $this->loadEnv;
        $loadEnv();

        // Check if we can connect to PostgreSQL
        $host = $_ENV['POSTGRES_HOST'] ?? 'localhost';
        $port = $_ENV['POSTGRES_PORT'] ?? '5432';
        $user = $_ENV['POSTGRES_USER'] ?? 'postgres';
        $password = $_ENV['POSTGRES_PASSWORD'] ?? 'postgres';

        try {
            // Connect to default 'postgres' database to create test template
            /** @var \PDO $masterPdo */
            $this->masterPdo = new PDO(
                "pgsql:host=$host;port=$port;dbname=postgres",
                $user,
                $password,
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
            );

            // Create a template database for tests if it doesn't exist
            /** @var string $templateDb */
            $this->templateDb = 'tonics_test_template';

            // Drop and recreate template database
            $this->masterPdo->exec("DROP DATABASE IF EXISTS {$this->templateDb}");
            $this->masterPdo->exec("CREATE DATABASE {$this->templateDb}");

            // Connect to template database and set up schema
            $templatePdo = new PDO(
                "pgsql:host=$host;port=$port;dbname={$this->templateDb}",
                $user,
                $password,
                [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
            );

            // Create test tables in template
            $templatePdo->exec("
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) UNIQUE NOT NULL,
                    email VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    logins INTEGER DEFAULT 0
                )
            ");

            $templatePdo->exec("
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    title VARCHAR(255),
                    content TEXT,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ");

            // Close template connection - we don't need it anymore
            $templatePdo = null;

            $this->postgresAvailable = true;
            $this->connectionParams = compact('host', 'port', 'user', 'password');

        } catch (PDOException $e) {
            var_dump($e->getMessage());
            exit(0);
            $this->postgresAvailable = false;
            skipIf(true, "PostgreSQL not available: " . $e->getMessage());
        }
    });

    afterAll(function() {
        if ($this->postgresAvailable && isset($this->masterPdo) && isset($this->templateDb)) {
            try {
                // Terminate all connections to template database before dropping
                $this->masterPdo->exec("
                    SELECT pg_terminate_backend(pid) 
                    FROM pg_stat_activity 
                    WHERE datname = '{$this->templateDb}' AND pid <> pg_backend_pid()
                ");
                $this->masterPdo->exec("DROP DATABASE IF EXISTS {$this->templateDb}");
            } catch (PDOException $e) {
                // Ignore cleanup errors
            }
        }
    });

    beforeEach(function() {
        skipIf(!$this->postgresAvailable, "PostgreSQL not available");

        // Create a unique test database from template
        $this->testDb = 'tonics_test_' . uniqid();

        extract($this->connectionParams);

        // Create test database from template (faster than recreating schema)
        $this->masterPdo->exec("CREATE DATABASE {$this->testDb} TEMPLATE {$this->templateDb}");

        // Connect to test database
        /** @var \PDO $pdo */
        $this->pdo = new PDO(
            "pgsql:host=$host;port=$port;dbname={$this->testDb}",
            $user,
            $password,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );

        /** @var PostgresTables $tables */
        $this->tables = new PostgresTables();
        $this->tables->addTable('users', ['id', 'username', 'email', 'created_at', 'logins']);
        $this->tables->addTable('posts', ['id', 'user_id', 'title', 'content', 'metadata', 'created_at']);

        /** @var PostgresTonicsQueryTransformer $query */
        $this->query = new PostgresTonicsQueryTransformer();

        /** @var TonicsQueryBuilder $qb */
        $this->qb = new TonicsQueryBuilder($this->pdo, $this->query, $this->tables);
        $this->query->setTonicsQueryBuilder($this->qb);
    });

    afterEach(function() {
        // Close PDO connection
        $this->pdo = null;

        if (isset($this->testDb) && isset($this->masterPdo)) {
            try {

                // Terminate all connections to test database
                $this->masterPdo->exec("
                    SELECT pg_terminate_backend(pid) 
                    FROM pg_stat_activity 
                    WHERE datname = '{$this->testDb}' AND pid <> pg_backend_pid()
                ");
                $this->masterPdo->exec("DROP DATABASE IF EXISTS {$this->testDb}");
            } catch (PDOException $e) {
                // Ignore cleanup errors
            }
        }
    });

    describe('Date/Time Operations', function() {

        beforeEach(function() {
            // Insert test data
            $this->pdo->exec("
                INSERT INTO users (username, email, created_at) VALUES 
                ('alice', 'alice@example.com', '2025-01-15 10:30:00'),
                ('bob', 'bob@example.com', '2024-12-20 14:45:00'),
                ('charlie', 'charlie@example.com', '2025-02-01 09:00:00')
            ");
        });

        describe('->WhereDate()', function() {

            it('filters by date using CAST', function() {
                $q = $this->qb->getNewQuery();
                $results = $q->Select('username')
                    ->From('users')
                    ->WhereDate('created_at', '>=', '2025-01-01')
                    ->OrderByAsc('"username"')
                    ->FetchResult();

                expect(count($results))->toBe(2);
                expect($results[0]->username)->toBe('alice');
                expect($results[1]->username)->toBe('charlie');
            });

            it('generates correct SQL with CAST', function() {
                $q = $this->qb->getNewQuery();
                $q->Select('*')->From('users')
                    ->WhereDate('created_at', '=', '2025-01-15');

                $sql = $q->getSqlString();
                expect($sql)->toContain('CAST(created_at AS DATE)');
            });

        });

        describe('->WhereTime()', function() {

            it('filters by time using CAST', function() {
                $q = $this->qb->getNewQuery();
                $results = $q->Select('username')
                    ->From('users')
                    ->WhereTime('created_at', '>=', '10:00:00')
                    ->OrderByAsc('"username"')
                    ->FetchResult();

                expect(count($results))->toBe(2);
            });

        });

        describe('->DateFormat()', function() {

            it('formats dates using TO_CHAR', function() {
                $q = $this->qb->getNewQuery();
                $q->Select()->DateFormat('created_at', 'YYYY-MM-DD')
                    ->As('formatted_date')
                    ->From('users')
                    ->Where('username', '=', 'alice');

                $result = $q->FetchFirst();
                expect($result->formatted_date)->toMatch('/2025-01-15/');
            });

            it('converts MySQL format tokens correctly', function() {
                $q = $this->qb->getNewQuery();
                $q->Select()->DateFormat('created_at', '%Y/%m/%d %H:%i:%s')
                    ->As('fmt')
                    ->From('users')
                    ->Where('username', '=', 'alice');

                $result = $q->FetchFirst();
                expect($result->fmt)->toMatch('/2025\/01\/15/');
            });

        });

    });

    describe('String Operations', function() {

        beforeEach(function() {
            $this->pdo->exec("
                INSERT INTO users (username, email) VALUES 
                ('john_doe', 'john@example.com'),
                ('jane_smith', 'jane@example.com'),
                ('admin_user', 'admin@example.com'),
                ('test123', 'test@example.com')
            ");
        });

        describe('->WhereLike()', function() {

            it('finds matches with wildcards using || operator', function() {
                $q = $this->qb->getNewQuery();
                $results = $q->Select('username')
                    ->From('users')
                    ->WhereLike('username', 'john')
                    ->FetchResult();

                expect(count($results))->toBe(1);
                expect($results[0]->username)->toBe('john_doe');
            });

            it('generates SQL with || concatenation', function() {
                $q = $this->qb->getNewQuery();
                $q->Select('*')->From('users')->WhereLike('username', 'test');

                $sql = $q->getSqlString();
                expect($sql)->toContain('||');
                expect($sql)->not->toContain('CONCAT');
            });

        });

        describe('->WhereStarts()', function() {

            it('finds matches that start with pattern', function() {
                $q = $this->qb->getNewQuery();
                $results = $q->Select('username')
                    ->From('users')
                    ->WhereStarts('username', 'admin')
                    ->FetchResult();

                expect(count($results))->toBe(1);
                expect($results[0]->username)->toBe('admin_user');
            });

        });

        describe('->WhereEnds()', function() {

            it('finds matches that end with pattern', function() {
                $q = $this->qb->getNewQuery();
                $results = $q->Select('username')
                    ->From('users')
                    ->WhereEnds('username', '123')
                    ->FetchResult();

                expect(count($results))->toBe(1);
                expect($results[0]->username)->toBe('test123');
            });

        });

    });

    describe('JSON Operations', function() {

        beforeEach(function() {
            $this->pdo->exec("INSERT INTO users (username, email) VALUES ('testuser', 'test@example.com')");
            $userId = $this->pdo->lastInsertId();

            $this->pdo->exec("
                INSERT INTO posts (user_id, title, content, metadata) VALUES 
                ($userId, 'First Post', 'Content 1', '{\"author\": \"John Doe\", \"tags\": [\"featured\", \"tech\"], \"stats\": {\"views\": 100}}'),
                ($userId, 'Second Post', 'Content 2', '{\"author\": \"Jane Smith\", \"tags\": [\"news\"], \"stats\": {\"views\": 50}}'),
                ($userId, 'Third Post', 'Content 3', '{\"tags\": [\"featured\", \"news\"]}')
            ");
        });

        describe('->JsonExtract()', function() {

            it('extracts JSON field using #>> operator', function() {
                $q = $this->qb->getNewQuery();
                $q->Select('title, ')
                    ->Select($q->Q()->JsonExtract('metadata', 'author'))->As('author')
                    ->From('posts')
                    ->Where('title', '=', 'First Post');

                $result = $q->FetchFirst();
                expect($result->author)->toBe('John Doe');
            });

            it('extracts nested JSON paths', function() {
                /** @var \Devsrealm\TonicsQueryBuilder\TonicsQuery $q */
                $q = $this->qb->getNewQuery();
                $q->Select()
                    ->JsonExtract('metadata', 'stats.views', '$.')->As('views')
                    ->From('posts')
                    ->Where('title', '=', 'First Post');

                $result = $q->FetchFirst();
                expect($result->views)->toBe('100');
            });

            it('generates SQL with #>> and ::jsonb', function() {
                $q = $this->qb->getNewQuery();
                $q->JsonExtract('metadata', 'author');

                $sql = $q->getSqlString();
                expect($sql)->toContain('#>>');
                expect($sql)->toContain('::jsonb');
            });

        });

        describe('->JsonSet()', function() {

            it('updates JSON field using jsonb_set', function() {
                $q = $this->qb->getNewQuery();
                $q->Update('posts')
                    ->Set('metadata', $q->Q()->JsonSet('metadata', 'author', '"Updated Author"'))
                    ->Where('title', '=', 'First Post')
                    ->Exec();

                // Verify update
                $check = $this->qb->getNewQuery();
                $check->Select()->JsonExtract('metadata', 'author')->As('author')
                    ->From('posts')
                    ->Where('title', '=', 'First Post');

                $result = $check->FetchFirst();
                expect($result->author)->toBe('Updated Author');
            });

            it('handles multiple path/value pairs', function() {
                $q = $this->qb->getNewQuery();
                $q->JsonSet('metadata', 'author', '"New Author"', 'title', '"New Title"');

                $sql = $q->getSqlString();
                $count = substr_count($sql, 'jsonb_set');
                expect($count)->toBe(2);
            });

            it('throws exception for odd number of parameters', function() {
                $q = $this->qb->getNewQuery();
                expect(function() use ($q) {
                    $q->JsonSet('metadata', 'author');
                })->toThrow(new InvalidArgumentException());
            });

        });

        describe('->JsonRemove()', function() {

            it('removes JSON paths using #- operator', function() {
                $q = $this->qb->getNewQuery();
                $q->Update('posts')
                    ->Set('metadata', $q->Q()->JsonRemove('metadata', 'author'))
                    ->Where('title', '=', 'First Post')
                    ->Exec();

                // Verify removal
                $check = $this->qb->getNewQuery();
                $check->Select('metadata')->From('posts')->Where('title', '=', 'First Post');
                $result = $check->FetchFirst();

                $meta = json_decode($result->metadata, true);
                expect(isset($meta['author']))->toBe(false);
            });

        });

        describe('->JsonContain()', function() {

            it('uses @> operator for containment check', function() {
                $q = $this->qb->getNewQuery();
                $q->JsonContain('metadata', 'tags', '["featured"]');

                $sql = $q->getSqlString();
                expect($sql)->toContain('@>');
            });

        });

        describe('->WhereJsonContains()', function() {

            it('filters by JSON containment', function() {
                $q = $this->qb->getNewQuery();
                $results = $q->Select('title')
                    ->From('posts')
                    ->WhereJsonContains('metadata', 'tags', '["featured"]', '$.')
                    ->OrderByAsc('"title"')
                    ->FetchResult();

                expect(count($results))->toBe(2);
                expect($results[0]->title)->toBe('First Post');
            });

        });

        describe('->JsonExist()', function() {

            it('checks if JSON path exists', function() {
                $q = $this->qb->getNewQuery();
                $q->JsonExist('metadata', 'author');

                $sql = $q->getSqlString();
                expect($sql)->toContain('IS NOT NULL');
            });

        });

        describe('->JsonMergePatch()', function() {

            it('merges JSON using || operator', function() {
                $q = $this->qb->getNewQuery();
                $q->JsonMergePatch('metadata', '{"new_field": "value"}');

                $sql = $q->getSqlString();
                expect($sql)->toContain('||');
                expect($sql)->toContain('::jsonb');
            });

        });

        describe('->JsonArrayAppend()', function() {

            it('throws RuntimeException with helpful message', function() {
                $q = $this->qb->getNewQuery();
                expect(function() use ($q) {
                    $q->JsonArrayAppend('data', [['$[0]' => 'value']]);
                })->toThrow(new RuntimeException());
            });

        });

    });

    describe('Operators', function() {

        beforeEach(function() {
            $this->pdo->exec("
                INSERT INTO users (username, email) VALUES 
                ('user1', 'user1@example.com'),
                ('user2', NULL),
                ('user3', NULL)
            ");
        });

        describe('->NullSafeEqualOperator()', function() {

            it('returns IS NOT DISTINCT FROM', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->NullSafeEqualOperator();
                expect($result)->toBe('IS NOT DISTINCT FROM');
            });

            it('matches NULL values correctly', function() {
                $q = $this->qb->getNewQuery();
                $results = $q->Select('username')
                    ->From('users')
                    ->Where('email', '<=>', null)
                    ->OrderByAsc('"username"')
                    ->FetchResult();

                expect(count($results))->toBe(2);
                expect($results[0]->username)->toBe('user2');
                expect($results[1]->username)->toBe('user3');
            });

            it('generates correct SQL', function() {
                $q = $this->qb->getNewQuery();
                $q->Select('*')->From('users')->Where('email', '<=>', null);

                $sql = $q->getSqlString();
                expect($sql)->toContain('IS NOT DISTINCT FROM');
            });

        });

    });

    describe('Upsert Operations', function() {

        describe('->InsertOnDuplicate()', function() {

            it('inserts new records using ON CONFLICT', function() {
                $q = $this->qb->getNewQuery();
                $data = [
                    ['username' => 'alice', 'email' => 'alice@example.com', 'logins' => 1],
                    ['username' => 'bob', 'email' => 'bob@example.com', 'logins' => 1]
                ];

                $result = $q->InsertOnDuplicate('users', $data, [
                    'conflict' => ['username'],
                    'set' => ['email', 'logins']
                ]);

                expect($result)->toBe(true);
                expect($q->getRowCount())->toBe(2);

                // Verify data
                $check = $this->qb->getNewQuery();
                $users = $check->Select('username, email')->From('users')->OrderByAsc('"username"')->FetchResult();
                expect(count($users))->toBe(2);
            });

            it('updates existing records on conflict', function() {
                // Insert initial data
                $this->pdo->exec("INSERT INTO users (username, email, logins) VALUES ('alice', 'old@example.com', 5)");

                // Upsert with conflict
                $q = $this->qb->getNewQuery();
                $data = [['username' => 'alice', 'email' => 'new@example.com', 'logins' => 10]];

                $result = $q->InsertOnDuplicate('users', $data, [
                    'conflict' => ['username'],
                    'set' => ['email', 'logins']
                ]);

                expect($result)->toBe(true);

                // Verify update
                $check = $this->qb->getNewQuery();
                $user = $check->Select('email, logins')->From('users')->Where('username', '=', 'alice')->FetchFirst();
                expect($user->email)->toBe('new@example.com');
                expect((int)$user->logins)->toBe(10);
            });

            it('works with constraint name', function() {
                $q = $this->qb->getNewQuery();
                $data = [['id' => 1, 'username' => 'test', 'email' => 'test@example.com']];

                $result = $q->InsertOnDuplicate('users', $data, [
                    'constraint' => 'users_pkey',
                    'set' => ['username', 'email']
                ]);

                expect($result)->toBe(true);
            });

            it('infers id column when present in data', function() {
                $q = $this->qb->getNewQuery();
                $data = [['id' => 999, 'username' => 'inferred', 'email' => 'inferred@example.com']];

                $result = $q->InsertOnDuplicate('users', $data, ['username', 'email']);

                expect($result)->toBe(true);

                // Verify insertion
                $check = $this->qb->getNewQuery();
                $user = $check->Select('username')->From('users')->Where('id', '=', 999)->FetchFirst();
                expect($user->username)->toBe('inferred');
            });

            it('throws exception when no conflict target specified', function() {
                $q = $this->qb->getNewQuery();
                $data = [['username' => 'alice', 'email' => 'alice@example.com']];

                expect(function() use ($q, $data) {
                    $q->InsertOnDuplicate('users', $data, ['email']);
                })->toThrow(new InvalidArgumentException());
            });

            it('throws exception when no columns to set', function() {
                $q = $this->qb->getNewQuery();
                $data = [['username' => 'alice']];

                expect(function() use ($q, $data) {
                    $q->InsertOnDuplicate('users', $data, [
                        'conflict' => ['username'],
                        'set' => []
                    ]);
                })->toThrow(new InvalidArgumentException());
            });

            it('handles batch inserts with chunking', function() {
                $q = $this->qb->getNewQuery();
                $data = [];
                for ($i = 1; $i <= 150; $i++) {
                    $data[] = ['username' => "user$i", 'email' => "user$i@example.com"];
                }

                $result = $q->InsertOnDuplicate('users', $data, [
                    'conflict' => ['username'],
                    'set' => ['email']
                ], 50);

                expect($result)->toBe(true);
                expect($q->getRowCount())->toBe(150);

                // Verify data
                $check = $this->qb->getNewQuery();
                $count = $check->Select('COUNT(*) AS total')
                    ->From('users')
                    ->FetchFirst();
                expect((int)$count->total)->toBe(150);
            });

            it('generates EXCLUDED references in SQL', function() {
                $this->pdo->exec("INSERT INTO users (username, email, logins) VALUES ('alice', 'old@example.com', 5)");

                $q = $this->qb->getNewQuery();
                $data = [['username' => 'alice', 'email' => 'new@example.com', 'logins' => 10]];

                $q->InsertOnDuplicate('users', $data, [
                    'conflict' => ['username'],
                    'set' => ['email', 'logins']
                ]);

                // Email and logins should be updated from EXCLUDED values
                $check = $this->qb->getNewQuery();
                $user = $check->Select('email, logins')->From('users')->Where('username', '=', 'alice')->FetchFirst();
                expect($user->email)->toBe('new@example.com');
                expect((int)$user->logins)->toBe(10);
            });

        });

    });

    describe('Integration Tests', function() {

        beforeEach(function() {
            $this->pdo->exec("
                INSERT INTO users (username, email, created_at) VALUES 
                ('alice', 'alice@example.com', '2025-01-15'),
                ('bob', 'bob@example.com', '2024-12-20')
            ");

            $userId = $this->pdo->query("SELECT id FROM users WHERE username = 'alice'")->fetchColumn();
            $this->pdo->exec("
                INSERT INTO posts (user_id, title, content, metadata) VALUES 
                ($userId, 'Test Post', 'Content', '{\"author\": \"Alice\", \"tags\": [\"tech\"]}')
            ");
        });

        it('builds and executes a complete SELECT query with Postgres syntax', function() {
            $q = $this->qb->getNewQuery();
            $users = $q->Select($this->tables->pickTable('users', ['id', 'username']))
                ->From($this->tables->getTable('users'))
                ->WhereDate('created_at', '>=', '2025-01-01')
                ->WhereLike('username', 'ali')
                ->OrderByAsc('"username"')
                ->Take(10)
                ->Skip(0)
                ->FetchResult();

            expect(count($users))->toBe(1);
            expect($users[0]->username)->toBe('alice');
        });

        it('builds and executes UPDATE query with JSON operations', function() {
            $q = $this->qb->getNewQuery();
            $jsonSet = $q->Q()->JsonSet('metadata', 'author', '"Bob"');
            $q->Update($this->tables->getTable('posts'))
                ->Set('metadata', $jsonSet)
                ->Where('title', '=', 'Test Post')
                ->Exec();

            // Verify update
            $check = $this->qb->getNewQuery();
            $check->Select('')
                ->Select($check->Q()->JsonExtract('metadata', 'author'))->As('author')
                ->From('posts')
                ->Where('title', '=', 'Test Post');

            $result = $check->FetchFirst();
            expect($result->author)->toBe('Bob');
        });

        it('handles complex queries with subqueries', function() {
            $q = $this->qb->getNewQuery();
            $subQuery = $this->qb->getNewQuery();
            $subQuery->Select('id')->From('users')->Where('username', '=', 'alice');

            $q->Select('title')
                ->From('posts')
                ->Where('user_id', '=', $subQuery);

            $results = $q->FetchResult();
            expect(count($results))->toBe(1);
            expect($results[0]->title)->toBe('Test Post');
        });

        it('executes transactions correctly', function() {
            $this->pdo->beginTransaction();

            try {
                $q1 = $this->qb->getNewQuery();
                $q1->Insert('users', [
                    'username' => 'charlie',
                    'email' => 'charlie@example.com'
                ]);

                $q2 = $this->qb->getNewQuery();
                $q2->Update('users')
                    ->Set('logins', $q2->Q()->Raw('logins + 1'))
                    ->Where('username', '=', 'charlie')
                    ->Exec();

                $this->pdo->commit();

                // Verify transaction
                $check = $this->qb->getNewQuery();
                $user = $check->Select('username, logins')->From('users')->Where('username', '=', 'charlie')->FetchFirst();
                expect($user->username)->toBe('charlie');
                expect((int)$user->logins)->toBe(1);

            } catch (Exception $e) {
                $this->pdo->rollBack();
                throw $e;
            }
        });

        it('handles pagination correctly', function() {
            // Insert more users
            for ($i = 3; $i <= 10; $i++) {
                $this->pdo->exec("INSERT INTO users (username, email) VALUES ('user$i', 'user$i@example.com')");
            }

            $paginated = $this->qb->getNewQuery();
            $results = $paginated->Select('username, email')
                ->From('users')
                ->OrderByAsc('"username"')
                ->SimplePaginate(5, 'page');

            expect($results)->not->toBe(null);
            expect($results->per_page)->toBe(5);
            expect(count($results->data))->toBe(5);
        });

    });

    describe('PostgreSQL-style placeholders', function() {

        beforeEach(function() {
            $this->pdo->exec("
                INSERT INTO users (username, email, logins, created_at) VALUES 
                ('alice', 'alice@example.com', 5, '2025-01-15'),
                ('bob', 'bob@example.com', 10, '2025-02-20'),
                ('charlie', NULL, 3, '2025-03-10')
            ");
        });

        describe('->runPg()', function() {

            it('converts $1 placeholder to PDO ? placeholder', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->runPg(
                    "SELECT username FROM users WHERE username = $1",
                    'alice'
                );

                expect(count($result))->toBe(1);
                expect($result[0]->username)->toBe('alice');
            });

            it('handles multiple PostgreSQL-style placeholders', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->runPg(
                    "SELECT username, logins FROM users WHERE logins >= $1 AND logins <= $2 ORDER BY username",
                    5,
                    10
                );

                expect(count($result))->toBe(2);
                expect($result[0]->username)->toBe('alice');
                expect($result[1]->username)->toBe('bob');
            });

            it('works with EXISTS subquery', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->runPg(
                    "SELECT EXISTS(SELECT 1 FROM users WHERE username = $1) AS result",
                    'alice'
                );

                expect($result[0]->result)->toBe(true);
            });

            it('handles NULL values correctly', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->runPg(
                    "SELECT username FROM users WHERE email IS NOT DISTINCT FROM $1",
                    null
                );

                expect(count($result))->toBe(1);
                expect($result[0]->username)->toBe('charlie');
            });

            it('works with ORDER BY and LIMIT', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->runPg(
                    "SELECT username FROM users WHERE logins >= $1 ORDER BY logins DESC LIMIT $2",
                    3,
                    2
                );

                expect(count($result))->toBe(2);
                expect($result[0]->username)->toBe('bob');
                expect($result[1]->username)->toBe('alice');
            });

            it('handles date comparisons', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->runPg(
                    "SELECT username FROM users WHERE created_at >= $1 ORDER BY created_at",
                    '2025-02-01'
                );

                expect(count($result))->toBe(2);
                expect($result[0]->username)->toBe('bob');
                expect($result[1]->username)->toBe('charlie');
            });

        });

        describe('->rowPg()', function() {

            it('returns single row with $1 placeholder', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->rowPg(
                    "SELECT username, email, logins FROM users WHERE username = $1",
                    'bob'
                );

                expect($result->username)->toBe('bob');
                expect($result->email)->toBe('bob@example.com');
                expect((int)$result->logins)->toBe(10);
            });

            it('handles multiple placeholders for single row', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->rowPg(
                    "SELECT username FROM users WHERE username = $1 AND logins > $2",
                    'alice',
                    3
                );

                expect($result->username)->toBe('alice');
            });

            it('returns false when no row found', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->rowPg(
                    "SELECT username FROM users WHERE username = $1",
                    'nonexistent'
                );

                expect($result)->toBe(false);
            });

            it('works with aggregate functions', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->rowPg(
                    "SELECT COUNT(*) as total FROM users WHERE logins >= $1",
                    5
                );

                expect((int)$result->total)->toBe(2);
            });

            it('handles complex WHERE conditions', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->rowPg(
                    "SELECT username, logins FROM users WHERE (logins > $1 OR email IS NULL) AND created_at > $2 ORDER BY logins DESC LIMIT 1",
                    4,
                    '2025-01-01'
                );

                expect($result->username)->toBe('bob');
                expect((int)$result->logins)->toBe(10);
            });

            it('works with CASE statements', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->rowPg(
                    "SELECT username, CASE WHEN logins >= $1 THEN 'active' ELSE 'inactive' END as status FROM users WHERE username = $2",
                    5,
                    'bob'
                );

                expect($result->username)->toBe('bob');
                expect($result->status)->toBe('active');
            });

        });

    });

    describe('Multiple SQL statements', function() {

        describe('->execRaw()', function() {

            it('executes multiple CREATE statements', function() {
                $q = $this->qb->getNewQuery();
                $result = $q->execRaw(<<<SQL
                    CREATE TABLE IF NOT EXISTS test_table1 (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255)
                    );
                    CREATE TABLE IF NOT EXISTS test_table2 (
                        id SERIAL PRIMARY KEY,
                        email VARCHAR(255)
                    );
SQL);

                expect($result)->toBeA('integer');

                // Verify tables were created
                $check = $this->pdo->query("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'test_table1')");
                expect($check->fetchColumn())->toBe(true);

                $check2 = $this->pdo->query("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'test_table2')");
                expect($check2->fetchColumn())->toBe(true);
            });

            it('executes schema creation and table creation', function() {
                $q = $this->qb->getNewQuery();
                $q->execRaw(<<<SQL
                    CREATE SCHEMA IF NOT EXISTS test_schema;
                    CREATE TABLE IF NOT EXISTS test_schema.users (
                        id UUID PRIMARY KEY,
                        username VARCHAR(255) NOT NULL
                    );
SQL);

                // Verify schema exists
                $check = $this->pdo->query("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'test_schema')");
                expect($check->fetchColumn())->toBe(true);

                // Verify table exists in schema
                $check2 = $this->pdo->query("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'test_schema' AND table_name = 'users')");
                expect($check2->fetchColumn())->toBe(true);
            });

            it('executes INSERT statements after CREATE', function() {
                $q = $this->qb->getNewQuery();
                $q->execRaw(<<<SQL
                    CREATE TABLE IF NOT EXISTS test_users (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(255) UNIQUE NOT NULL
                    );
                    INSERT INTO test_users (username) VALUES ('alice');
                    INSERT INTO test_users (username) VALUES ('bob');
SQL);

                // Verify data was inserted
                $check = $this->pdo->query("SELECT COUNT(*) FROM test_users");
                expect((int)$check->fetchColumn())->toBe(2);
            });

            it('handles CREATE INDEX and ALTER TABLE', function() {
                $q = $this->qb->getNewQuery();
                $q->execRaw(<<<SQL
                    CREATE TABLE IF NOT EXISTS indexed_table (
                        id SERIAL PRIMARY KEY,
                        email VARCHAR(255),
                        status VARCHAR(50)
                    );
                    CREATE INDEX IF NOT EXISTS idx_email ON indexed_table(email);
                    ALTER TABLE indexed_table ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
SQL);

                // Verify table exists
                $check = $this->pdo->query("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'indexed_table')");
                expect($check->fetchColumn())->toBe(true);

                // Verify column was added
                $check2 = $this->pdo->query("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'indexed_table' AND column_name = 'created_at')");
                expect($check2->fetchColumn())->toBe(true);
            });

            it('throws exception on SQL syntax error', function() {
                $q = $this->qb->getNewQuery();

                expect(function() use ($q) {
                    $q->execRaw(<<<SQL
                        CREATE TABLE invalid syntax here;
                        SELECT * FROM nonexistent;
SQL);
                })->toThrow();
            });

            it('executes with comments in SQL', function() {
                $q = $this->qb->getNewQuery();
                $q->execRaw(<<<SQL
                    -- This is a comment
                    CREATE TABLE IF NOT EXISTS commented_table (
                        id SERIAL PRIMARY KEY,
                        /* Multi-line
                           comment */
                        name VARCHAR(255)
                    );
                    -- Another comment
                    INSERT INTO commented_table (name) VALUES ('test');
SQL);

                $check = $this->pdo->query("SELECT COUNT(*) FROM commented_table");
                expect((int)$check->fetchColumn())->toBe(1);
            });

        });

    });

});

