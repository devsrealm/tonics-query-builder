# Running Tests

This project uses [Kahlan](https://kahlan.github.io/docs/) for testing.

## Installation

Install test dependencies:

```bash
composer install
```

## PostgreSQL Integration Tests

The Postgres transformer tests use real PostgreSQL databases for integration testing. Each test creates a temporary database from a template, runs the test, and then cleans up.

### Prerequisites

1. **PostgreSQL 9.5+** installed and running
2. **PostgreSQL connection** accessible from the test environment
3. **Test user** with database creation privileges

### Environment Configuration

The test suite uses **phpdotenv** to load PostgreSQL connection settings. A `loadEnv()` helper function is available to all specs via Kahlan's scope system.

**How it works:**

The `kahlan-config.php` creates a `loadEnv()` function in the root scope that specs can access:

```php
// In your spec's beforeAll block
beforeAll(function() {
    // Get the root scope and load environment
    $loadEnv = $this->loadEnv;
    $loadEnv(); // Loads .env.test or .env
    
    // Now environment variables are available
    $host = getenv('POSTGRES_HOST') ?: 'localhost';
});
```

**Configuration priority:**

1. `.env.test` (preferred for testing - checked first)
2. `.env` (fallback if .env.test doesn't exist)
3. Default values in code (if no .env files exist)

**Setup:**

Edit the .env.test with your own credentials, e.g
```bash
cat > .env.test << EOF
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
EOF
```

**Available variables:**

```bash
POSTGRES_HOST=localhost      # PostgreSQL host
POSTGRES_PORT=5432          # PostgreSQL port
POSTGRES_USER=postgres      # Database user
POSTGRES_PASSWORD=postgres  # Database password
```

**Note:** Both `.env` and `.env.test` are git-ignored for security. The `loadEnv()` function uses a static variable to ensure it only loads once per test run.

### Default Configuration

If no `.env` files are found, tests use these defaults:
- Host: `localhost`
- Port: `5432`
- User: `postgres`
- Password: `postgres`

### Running Tests

Run all tests:

```bash
./vendor/bin/kahlan
```

Run only Postgres transformer tests:

```bash
./vendor/bin/kahlan --spec=spec/Transformers/Postgres
```

Run with coverage (requires Xdebug or phpdbg):

```bash
# With Xdebug (if installed)
./vendor/bin/kahlan --coverage=3

# With phpdbg (built into PHP)
phpdbg -qrr ./vendor/bin/kahlan --coverage=3
```

**Note:** If coverage is not available, tests will run normally with a warning message. Coverage is optional.

Run specific test file:

```bash
./vendor/bin/kahlan --spec=spec/Transformers/Postgres/PostgresTablesSpec.php
```

### Kahlan Lifecycle Functions

Kahlan provides these lifecycle functions:
- `beforeAll` - Run once before all specs in a describe/context block
- `afterAll` - Run once after all specs in a describe/context block
- `beforeEach` - Run before each individual spec
- `afterEach` - Run after each individual spec

### How It Works

The test suite uses a template database approach for fast test execution:

1. **Before all tests**: Creates a template database (`tonics_test_template`) with the schema
2. **Before each test**: Creates a unique test database from the template (fast operation)
3. **Test execution**: Runs against the isolated test database
4. **After each test**: Drops the test database

This approach is much faster than recreating the schema for each test and provides complete isolation between tests.

### Skipping Tests

If PostgreSQL is not available, the tests will automatically skip with a message:

```
PostgreSQL not available: SQLSTATE[...] 
```

### Troubleshooting

**Connection refused:**
```bash
# Check if PostgreSQL is running
pg_isready -h localhost -p 5432

# Start PostgreSQL (varies by system)
sudo systemctl start postgresql  # Linux
brew services start postgresql   # macOS
```

**Authentication failed:**
```bash
# Update pg_hba.conf to allow password authentication
# Add/modify this line:
# host    all    all    127.0.0.1/32    md5

# Restart PostgreSQL after changes
```

**Permission denied to create database:**
```sql
-- Grant createdb permission to test user
ALTER USER postgres CREATEDB;
```

**Port already in use:**
```bash
# Use a different port
export POSTGRES_PORT=5433
```

### Docker Setup (Optional)

Run PostgreSQL in Docker for testing:

```bash
# Start PostgreSQL container
docker run -d \
  --name tonics-test-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:15-alpine

# Create .env.test (already configured for Docker defaults)
cp .env.example .env.test

# Run tests
./vendor/bin/kahlan

# Cleanup
docker stop tonics-test-postgres
docker rm tonics-test-postgres
```

**Custom Docker configuration:**

```bash
# Start on different port
docker run -d \
  --name tonics-test-postgres \
  -e POSTGRES_PASSWORD=mypassword \
  -p 5433:5432 \
  postgres:15-alpine

# Update .env.test
cat > .env.test << EOF
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=postgres
POSTGRES_PASSWORD=mypassword
EOF
```

### CI/CD Integration

Example GitHub Actions workflow:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup PHP
        uses: shivammathur/setup-php@v2
        with:
          php-version: '8.1'
          extensions: pdo, pdo_pgsql
      
      - name: Install dependencies
        run: composer install
      
      - name: Create test config
        run: cp .env.example .env.test
      
      - name: Run tests
        run: vendor/bin/kahlan
```

**Alternative: Use environment variables directly**

```yaml
      - name: Run tests
        env:
          POSTGRES_HOST: localhost
          POSTGRES_PORT: 5432
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
        run: vendor/bin/kahlan
```

## Test Coverage

To generate coverage reports, you need either Xdebug or phpdbg:

```bash
# With Xdebug
./vendor/bin/kahlan --coverage=4

# With phpdbg
phpdbg -qrr ./vendor/bin/kahlan --coverage=4
```

Coverage levels:
- `0`: No coverage
- `1`: Summary only
- `2`: Class coverage
- `3`: Method coverage
- `4`: Line coverage (detailed)

## Writing Tests

Follow the existing test patterns in `spec/Transformers/Postgres/`:

```php
describe('Feature', function() {
    
    beforeAll(function() {
        // Setup - runs once before all tests in this block
    });
    
    beforeEach(function() {
        // Setup - runs before each test
        // Test database is already created and available as $this->pdo
    });
    
    afterEach(function() {
        // Cleanup - runs after each test
    });
    
    afterAll(function() {
        // Cleanup - runs once after all tests in this block
    });
    
    it('does something', function() {
        // Test code
        expect($result)->toBe($expected);
    });
    
});
```

See [Kahlan documentation](https://kahlan.github.io/docs/) for more details on writing tests.

