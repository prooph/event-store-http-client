<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2020 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2018-2020 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStoreHttpClient;

use Prooph\EventStore\EndPoint;
use Prooph\EventStore\Transport\Http\EndpointExtensions;
use Prooph\EventStore\UserCredentials;
use Psr\Log\LoggerInterface as Logger;
use Psr\Log\NullLogger;

class ConnectionSettings
{
    private Logger $log;
    private bool $verboseLogging;
    private EndPoint $endPoint;
    private string $schema;
    private ?UserCredentials $defaultUserCredentials;
    private bool $requireMaster;

    public static function default(): ConnectionSettings
    {
        return new self(
            new NullLogger(),
            false,
            new EndPoint('localhost', 2113),
            EndpointExtensions::HTTP_SCHEMA,
            null,
            true
        );
    }

    public function __construct(
        Logger $logger,
        bool $verboseLogging,
        EndPoint $endpoint,
        string $schema = EndpointExtensions::HTTP_SCHEMA,
        ?UserCredentials $defaultUserCredentials = null,
        bool $requireMaster = true
    ) {
        $this->log = $logger;
        $this->verboseLogging = $verboseLogging;
        $this->endPoint = $endpoint;
        $this->schema = $schema;
        $this->defaultUserCredentials = $defaultUserCredentials;
        $this->requireMaster = $requireMaster;
    }

    /** @psalm-pure */
    public function defaultUserCredentials(): ?UserCredentials
    {
        return $this->defaultUserCredentials;
    }

    /** @psalm-pure */
    public function schema(): string
    {
        return $this->schema;
    }

    /** @psalm-pure */
    public function endPoint(): EndPoint
    {
        return $this->endPoint;
    }

    /** @psalm-pure */
    public function requireMaster(): bool
    {
        return $this->requireMaster;
    }

    /** @psalm-pure */
    public function log(): Logger
    {
        return $this->log;
    }

    /** @psalm-pure */
    public function verboseLogging(): bool
    {
        return $this->verboseLogging;
    }
}
