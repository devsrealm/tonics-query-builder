<?php

use Kahlan\Filter\Filters;
use Kahlan\Reporter\Coverage;
use Kahlan\Reporter\Coverage\Driver\Xdebug;
use Kahlan\Reporter\Coverage\Driver\Phpdbg;

// Create a helper function to load environment variables
$loadEnvHelper = function($next) {
    $scope = $this->suite()->root()->scope(); // The top most describe scope

    // Make loadEnv available to all specs via the scope
    $scope->loadEnv = function() {
        static $loaded = false;

        if ($loaded) {
            return; // Only load once
        }

        $dotenvFiles = ['.env.test', '.env'];
        foreach ($dotenvFiles as $file) {
            $path = __DIR__ . '/' . $file;
            if (file_exists($path)) {
                $dotenv = Dotenv\Dotenv::createImmutable(__DIR__, $file);
                $dotenv->safeLoad();
                $loaded = true;
                break;
            }
        }
    };

    return $next();
};

Filters::apply($this, 'run', $loadEnvHelper);

$commandLine = $this->commandLine();
$commandLine->option('ff', 'default', 1);
$commandLine->option('reporter', 'default', 'verbose');

// Configure coverage only if explicitly requested and available
if ($commandLine->get('coverage')) {
    Filters::apply($this, 'reporting', function($next) {
        $hasCoverage = extension_loaded('xdebug') || PHP_SAPI === 'phpdbg';

        if (!$hasCoverage) {
            echo "\n\033[33mWarning: Code coverage requested but not available.\033[0m\n";
            echo "Install Xdebug or run with phpdbg to enable coverage.\n";
            echo "  - With Xdebug: ./vendor/bin/kahlan --coverage=3\n";
            echo "  - With phpdbg: phpdbg -qrr ./vendor/bin/kahlan --coverage=3\n\n";
            return $next();
        }

        $reporter = new Coverage([
            'verbosity' => $this->commandLine()->get('coverage'),
            'driver'    => extension_loaded('xdebug') ? new Xdebug() : new Phpdbg(),
            'path'      => [
                'src/Transformers/Postgres',
            ],
            'exclude' => [
                'vendor',
                'spec',
            ],
            'colors'    => !$this->commandLine()->get('no-colors')
        ]);

        $this->reporters()->add('coverage', $reporter);

        return $next();
    });
}


