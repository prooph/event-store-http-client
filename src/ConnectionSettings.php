<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2019 prooph software GmbH <contact@prooph.de>
 * (c) 2018-2019 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStoreHttpClient;

use Prooph\EventStore\EndPoint;
use Prooph\EventStore\Transport\Http\EndpointExtensions;
use Prooph\EventStore\UserCredentials;

class ConnectionSettings
{
    /** @var EndPoint */
    private $endPoint;
    /** @var string */
    private $schema;
    /** @var UserCredentials|null */
    private $defaultUserCredentials;
    /** @var bool */
    private $requireMaster;

    public static function default(): ConnectionSettings
    {
        return new self(
            new EndPoint('localhost', 2113),
            EndpointExtensions::HTTP_SCHEMA,
            null,
            true
        );
    }

    public function __construct(
        EndPoint $endpoint,
        string $schema = EndpointExtensions::HTTP_SCHEMA,
        ?UserCredentials $defaultUserCredentials = null,
        bool $requireMaster = true
    ) {
        $this->endPoint = $endpoint;
        $this->schema = $schema;
        $this->defaultUserCredentials = $defaultUserCredentials;
        $this->requireMaster = $requireMaster;
    }

    public function defaultUserCredentials(): ?UserCredentials
    {
        return $this->defaultUserCredentials;
    }

    public function schema(): string
    {
        return $this->schema;
    }

    public function endPoint(): EndPoint
    {
        return $this->endPoint;
    }

    public function requireMaster(): bool
    {
        return $this->requireMaster;
    }
}
