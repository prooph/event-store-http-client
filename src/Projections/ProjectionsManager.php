<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2018-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStoreHttpClient\Projections;

use Prooph\EventStoreHttpClient\EndPoint;
use Prooph\EventStoreHttpClient\Exception\InvalidArgumentException;
use Prooph\EventStoreHttpClient\Http\EndpointExtensions;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Prooph\EventStoreHttpClient\UserCredentials;

class ProjectionsManager
{
    /** @var ProjectionsClient */
    private $client;
    /** @var EndPoint */
    private $httpEndPoint;
    /** @var string */
    private $httpSchema;
    /** @var UserCredentials|null */
    private $defaultUserCredentials;

    /** @internal */
    public function __construct(
        HttpClient $client,
        EndPoint $endPoint,
        string $schema = EndpointExtensions::HTTP_SCHEMA,
        ?UserCredentials $defaultUserCredentials = null
    ) {
        $this->client = new ProjectionsClient($client);
        $this->httpEndPoint = $endPoint;
        $this->httpSchema = $schema;
        $this->defaultUserCredentials = $defaultUserCredentials;
    }

    /**
     * Enables a projection
     */
    public function enable(string $name, ?UserCredentials $userCredentials = null): void
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        $this->client->enable(
            $this->httpEndPoint,
            $name,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Aborts and disables a projection without writing a checkpoint
     */
    public function disable(string $name, ?UserCredentials $userCredentials = null): void
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        $this->client->disable(
            $this->httpEndPoint,
            $name,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Disables a projection
     */
    public function abort(string $name, ?UserCredentials $userCredentials = null): void
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        $this->client->abort(
            $this->httpEndPoint,
            $name,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Creates a one-time query
     */
    public function createOneTime(
        string $query,
        ?UserCredentials $userCredentials = null
    ): void {
        if ('' === $query) {
            throw new InvalidArgumentException('Query is required');
        }

        $this->client->createOneTime(
            $this->httpEndPoint,
            $query,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Creates a one-time query
     */
    public function createTransient(
        string $name,
        string $query,
        ?UserCredentials $userCredentials = null
    ): void {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        if ('' === $query) {
            throw new InvalidArgumentException('Query is required');
        }

        $this->client->createTransient(
            $this->httpEndPoint,
            $name,
            $query,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Creates a continuous projection
     */
    public function createContinuous(
        string $name,
        string $query,
        bool $trackEmittedStreams = false,
        ?UserCredentials $userCredentials = null
    ): void {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        if ('' === $query) {
            throw new InvalidArgumentException('Query is required');
        }

        $this->client->createContinuous(
            $this->httpEndPoint,
            $name,
            $query,
            $trackEmittedStreams,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Lists all projections
     *
     * @return ProjectionDetails[]
     */
    public function listAll(?UserCredentials $userCredentials = null): array
    {
        return $this->client->listAll(
            $this->httpEndPoint,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Lists all one-time projections
     *
     * @return ProjectionDetails[]
     */
    public function listOneTime(?UserCredentials $userCredentials = null): array
    {
        return $this->client->listOneTime(
            $this->httpEndPoint,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Lists this status of all continuous projections
     *
     * @return ProjectionDetails[]
     */
    public function listContinuous(?UserCredentials $userCredentials = null): array
    {
        return $this->client->listContinuous(
            $this->httpEndPoint,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Returns String of JSON containing projection status
     */
    public function getStatus(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->client->getStatus(
            $this->httpEndPoint,
            $name,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Returns String of JSON containing projection state
     */
    public function getState(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->client->getState(
            $this->httpEndPoint,
            $name,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Returns String of JSON containing projection state
     */
    public function getPartitionState(
        string $name,
        string $partition,
        ?UserCredentials $userCredentials = null
    ): string {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        if ('' === $partition) {
            throw new InvalidArgumentException('Partition is required');
        }

        return $this->client->getPartitionState(
            $this->httpEndPoint,
            $name,
            $partition,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Returns String of JSON containing projection result
     */
    public function getResult(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->client->getResult(
            $this->httpEndPoint,
            $name,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Returns String of JSON containing projection result
     */
    public function getPartitionResult(
        string $name,
        string $partition,
        ?UserCredentials $userCredentials = null
    ): string {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        if ('' === $partition) {
            throw new InvalidArgumentException('Partition is required');
        }

        return $this->client->getPartitionResult(
            $this->httpEndPoint,
            $name,
            $partition,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    /**
     * Returns String of JSON containing projection statistics
     */
    public function getStatistics(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->client->getStatistics(
            $this->httpEndPoint,
            $name,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    public function getQuery(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->client->getQuery(
            $this->httpEndPoint,
            $name,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    public function updateQuery(
        string $name,
        string $query,
        bool $emitEnabled = false,
        ?UserCredentials $userCredentials = null
    ): void {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        if ('' === $query) {
            throw new InvalidArgumentException('Query is required');
        }

        $this->client->updateQuery(
            $this->httpEndPoint,
            $name,
            $query,
            $emitEnabled,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    public function delete(
        string $name,
        bool $deleteEmittedStreams = false,
        ?UserCredentials $userCredentials = null
    ): void {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        $this->client->delete(
            $this->httpEndPoint,
            $name,
            $deleteEmittedStreams,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }

    public function reset(string $name, ?UserCredentials $userCredentials = null): void
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        $this->client->reset(
            $this->httpEndPoint,
            $name,
            $userCredentials ?? $this->defaultUserCredentials,
            $this->httpSchema
        );
    }
}
