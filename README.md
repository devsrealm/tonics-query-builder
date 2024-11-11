```markdown
# Tonics Query Builder

Tonics SQL Query Builder is a library for building SQL in a modular and hookable manner.

## Installation

To install the library, use Composer:

```bash
composer require devsrealm/tonics-query-builder
```

## Requirements

- PHP
- PDO extension

## Usage

### Basic Usage

```php
use Devsrealm\TonicsQueryBuilder\TonicsQuery;

// Create a new instance of TonicsQuery
$query = new TonicsQuery();

// Example of running a query
$result = $query->query('SELECT * FROM users WHERE id = ?', 1);
print_r($result);
```

### Transactions

```php
use Devsrealm\TonicsQueryBuilder\TonicsQuery;

$query = new TonicsQuery();

try {
    $query->beginTransaction();
    
    // Your database operations here
    $query->query('INSERT INTO users (name) VALUES (?)', 'John Doe');
    
    $query->commit();
} catch (\Exception $e) {
    $query->rollBack();
    throw $e;
}
```

### Fetching Results

```php
use Devsrealm\TonicsQueryBuilder\TonicsQuery;

$query = new TonicsQuery();

// Fetch all results
$results = $query->FetchResult();
print_r($results);

// Fetch the first result
$firstResult = $query->FetchFirst();
print_r($firstResult);
```

## License

Tonics Query Builder is open-sourced software licensed under the [MIT license](LICENSE).
```