<?php

/**
 * @noinspection PhpUndefinedFunctionInspection
 * @noinspection PhpUndefinedMethodInspection
 */

use Devsrealm\TonicsQueryBuilder\Transformers\Postgres\PostgresTables;

/**
 * @property PostgresTables $tables
 */
describe('PostgresTables', function() {

    beforeEach(function() {
        $this->tables = new PostgresTables();
    });

    describe('->transformTableColumn()', function() {

        it('quotes a simple column name with double quotes', function() {
            $result = $this->tables->transformTableColumn('', 'username');
            expect($result)->toBe('"username"');
        });

        it('quotes table and column with double quotes', function() {
            $result = $this->tables->transformTableColumn('users', 'username');
            expect($result)->toBe('"users"."username"');
        });

        it('handles schema-qualified table names', function() {
            $result = $this->tables->transformTableColumn('public.users', 'username');
            expect($result)->toBe('"public"."users"."username"');
        });

        it('escapes double quotes in column names', function() {
            $result = $this->tables->transformTableColumn('', 'user"name');
            expect($result)->toBe('"user""name"');
        });

        it('escapes double quotes in table names', function() {
            $result = $this->tables->transformTableColumn('my"table', 'column');
            expect($result)->toBe('"my""table"."column"');
        });

        it('handles multi-level schema qualification', function() {
            $result = $this->tables->transformTableColumn('schema.public.users', 'id');
            expect($result)->toBe('"schema"."public"."users"."id"');
        });

        it('handles empty parts in table name', function() {
            $result = $this->tables->transformTableColumn('.users', 'id');
            expect($result)->toBe('"users"."id"');
        });

    });

    describe('->addTable()', function() {

        it('adds a table with columns', function() {
            $this->tables->addTable('users', ['id', 'username', 'email']);
            expect($this->tables->isTable('users'))->toBe(true);
        });

        it('stores columns correctly', function() {
            $this->tables->addTable('users', ['id', 'username']);
            expect($this->tables->hasColumn('users', 'id'))->toBe(true);
            expect($this->tables->hasColumn('users', 'username'))->toBe(true);
            expect($this->tables->hasColumn('users', 'nonexistent'))->toBe(false);
        });

        it('returns instance for method chaining', function() {
            $result = $this->tables->addTable('users', ['id']);
            expect($result)->toBe($this->tables);
        });

    });

    describe('->getTable()', function() {

        it('returns table name with prefix', function() {
            $tables = new PostgresTables('public.');
            $tables->addTable('users', ['id']);
            expect($tables->getTable('users'))->toBe('public.users');
        });

        it('returns table name without prefix when prefix is empty', function() {
            $this->tables->addTable('users', ['id']);
            expect($this->tables->getTable('users'))->toBe('users');
        });

        it('throws exception for non-existent table', function() {
            expect(function() {
                $this->tables->getTable('nonexistent');
            })->toThrow(new InvalidArgumentException());
        });

    });

    describe('->pick()', function() {

        beforeEach(function() {
            $this->tables->addTable('users', ['id', 'username', 'email', 'created_at']);
            $this->tables->addTable('posts', ['id', 'user_id', 'title', 'content']);
        });

        it('picks specified columns from a table', function() {
            $result = $this->tables->pick(['users' => ['id', 'username']]);
            expect($result)->toBe('"users"."id", "users"."username"');
        });

        it('picks columns from multiple tables', function() {
            $result = $this->tables->pick([
                'users' => ['id', 'username'],
                'posts' => ['id', 'title']
            ]);
            expect($result)->toBe('"users"."id", "users"."username", "posts"."id", "posts"."title"');
        });

        it('ignores non-existent columns', function() {
            $result = $this->tables->pick(['users' => ['id', 'nonexistent', 'username']]);
            expect($result)->toBe('"users"."id", "users"."username"');
        });

        it('throws exception if columns are not an array', function() {
            expect(function() {
                $this->tables->pick(['users' => 'id']);
            })->toThrow(new Exception());
        });

    });

    describe('->pickTable()', function() {

        it('picks columns from a single table', function() {
            $this->tables->addTable('users', ['id', 'username', 'email']);
            $result = $this->tables->pickTable('users', ['id', 'username']);
            expect($result)->toBe('"users"."id", "users"."username"');
        });

    });

    describe('->except()', function() {

        beforeEach(function() {
            $this->tables->addTable('users', ['id', 'username', 'email', 'created_at']);
        });

        it('picks all columns except specified ones', function() {
            $result = $this->tables->except(['users' => ['email', 'created_at']]);
            expect($result)->toBe('"users"."id", "users"."username"');
        });

        it('works with multiple tables', function() {
            $this->tables->addTable('posts', ['id', 'user_id', 'title']);
            $result = $this->tables->except([
                'users' => ['email', 'created_at'],
                'posts' => ['user_id']
            ]);
            expect($result)->toBe('"users"."id", "users"."username", "posts"."id", "posts"."title"');
        });

    });

    describe('->pickTableExcept()', function() {

        it('picks all columns from a table except specified ones', function() {
            $this->tables->addTable('users', ['id', 'username', 'email', 'created_at']);
            $result = $this->tables->pickTableExcept('users', ['email', 'created_at']);
            expect($result)->toBe('"users"."id", "users"."username"');
        });

    });

    describe('->getColumn()', function() {

        it('gets a single column from a table', function() {
            $this->tables->addTable('users', ['id', 'username']);
            $result = $this->tables->getColumn('users', 'username');
            expect($result)->toBe('"users"."username"');
        });

    });

    describe('->getAllColumn()', function() {

        it('returns all columns from all tables', function() {
            $this->tables->addTable('users', ['id', 'username']);
            $this->tables->addTable('posts', ['id', 'title']);
            $result = $this->tables->getAllColumn();
            expect($result)->toContain('"users"."id"');
            expect($result)->toContain('"users"."username"');
            expect($result)->toContain('"posts"."id"');
            expect($result)->toContain('"posts"."title"');
        });

    });

    describe('->setTablePrefix() and ->getTablePrefix()', function() {

        it('sets and gets table prefix', function() {
            $this->tables->setTablePrefix('public.');
            expect($this->tables->getTablePrefix())->toBe('public.');
        });

        it('uses prefix in getTable()', function() {
            $this->tables->setTablePrefix('myschema.');
            $this->tables->addTable('users', ['id']);
            expect($this->tables->getTable('users'))->toBe('myschema.users');
        });

    });

    describe('->hasColumn()', function() {

        it('returns true for existing column', function() {
            $this->tables->addTable('users', ['id', 'username']);
            expect($this->tables->hasColumn('users', 'username'))->toBe(true);
        });

        it('returns false for non-existing column', function() {
            $this->tables->addTable('users', ['id']);
            expect($this->tables->hasColumn('users', 'username'))->toBe(false);
        });

        it('returns false for non-existing table', function() {
            expect($this->tables->hasColumn('users', 'id'))->toBe(false);
        });

    });

});

