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

namespace Prooph\EventStoreHttpClient\Projections;

use Http\Message\RequestFactory;
use Prooph\EventStore\Exception\EventStoreConnectionException;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Projections\ProjectionDetails;
use Prooph\EventStore\Projections\ProjectionsManager as SyncProjectionsManager;
use Prooph\EventStore\Transport\Http\HttpStatusCode;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStore\Util\Json;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Prooph\EventStoreHttpClient\Exception\ProjectionCommandConflictException;
use Prooph\EventStoreHttpClient\Exception\ProjectionCommandFailedException;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class ProjectionsManager implements SyncProjectionsManager
{
    /** @var HttpClient */
    private $httpClient;
    /** @var ConnectionSettings */
    private $settings;

    /** @internal */
    public function __construct(
        ClientInterface $client,
        RequestFactory $requestFactory,
        ConnectionSettings $settings
    ) {
        $this->settings = $settings;

        $this->httpClient = new HttpClient(
            $client,
            $requestFactory,
            $settings,
            \sprintf(
                '%s://%s:%s',
                $settings->schema(),
                $settings->endPoint()->host(),
                $settings->endPoint()->port()
            )
        );
    }

    /**
     * Enables a projection
     */
    public function enable(string $name, ?UserCredentials $userCredentials = null): void
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        $this->sendPost(
            \sprintf(
                '/projection/%s/command/enable',
                \urlencode($name)
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
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

        $this->sendPost(
            \sprintf(
                '/projection/%s/command/disable',
                \urlencode($name)
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
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

        $this->sendPost(
            \sprintf(
                '/projection/%s/command/abort',
                \urlencode($name)
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    /**
     * Creates a one-time query
     */
    public function createOneTime(
        string $query,
        string $type = 'JS',
        ?UserCredentials $userCredentials = null
    ): void {
        if ('' === $query) {
            throw new InvalidArgumentException('Query is required');
        }

        $this->sendPost(
            \sprintf(
                '/projections/onetime?type=%s',
                $type
            ),
            $query,
            $userCredentials,
            HttpStatusCode::CREATED
        );
    }

    /**
     * Creates a one-time query
     */
    public function createTransient(
        string $name,
        string $query,
        string $type = 'JS',
        ?UserCredentials $userCredentials = null
    ): void {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        if ('' === $query) {
            throw new InvalidArgumentException('Query is required');
        }

        $this->sendPost(
            \sprintf(
                '/projections/transient?name=%s&type=%s',
                \urlencode($name),
                $type
            ),
            $query,
            $userCredentials,
            HttpStatusCode::CREATED
        );
    }

    /**
     * Creates a continuous projection
     */
    public function createContinuous(
        string $name,
        string $query,
        bool $trackEmittedStreams = false,
        string $type = 'JS',
        ?UserCredentials $userCredentials = null
    ): void {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        if ('' === $query) {
            throw new InvalidArgumentException('Query is required');
        }

        $this->sendPost(
            \sprintf(
                '/projections/continuous?name=%s&type=%s&emit=1&trackemittedstreams=%d',
                \urlencode($name),
                $type,
                (int) $trackEmittedStreams
            ),
            $query,
            $userCredentials,
            HttpStatusCode::CREATED
        );
    }

    /**
     * Lists all projections
     *
     * @return ProjectionDetails[]
     */
    public function listAll(?UserCredentials $userCredentials = null): array
    {
        $response = $this->sendGet(
            '/projections/any',
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($response->getBody()->getContents());

        $projectionDetails = [];

        if (null === $data['projections']) {
            return $projectionDetails;
        }

        foreach ($data['projections'] as $entry) {
            $projectionDetails[] = $this->buildProjectionDetails($entry);
        }

        return $projectionDetails;
    }

    /**
     * Lists all one-time projections
     *
     * @return ProjectionDetails[]
     */
    public function listOneTime(?UserCredentials $userCredentials = null): array
    {
        $response = $this->sendGet(
            '/projections/onetime',
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($response->getBody()->getContents());

        $projectionDetails = [];

        if (null === $data['projections']) {
            return $projectionDetails;
        }

        foreach ($data['projections'] as $entry) {
            $projectionDetails[] = $this->buildProjectionDetails($entry);
        }

        return $projectionDetails;
    }

    /**
     * Lists this status of all continuous projections
     *
     * @return ProjectionDetails[]
     */
    public function listContinuous(?UserCredentials $userCredentials = null): array
    {
        $response = $this->sendGet(
            '/projections/continuous',
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($response->getBody()->getContents());

        $projectionDetails = [];

        if (null === $data['projections']) {
            return $projectionDetails;
        }

        foreach ($data['projections'] as $entry) {
            $projectionDetails[] = $this->buildProjectionDetails($entry);
        }

        return $projectionDetails;
    }

    /**
     * Returns String of JSON containing projection status
     */
    public function getStatus(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->sendGet(
            \sprintf(
                '/projection/%s',
                \urlencode($name)
            ),
            $userCredentials,
            HttpStatusCode::OK
        )->getBody()->getContents();
    }

    /**
     * Returns String of JSON containing projection state
     */
    public function getState(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->sendGet(
            \sprintf(
                '/projection/%s/state',
                \urlencode($name)
            ),
            $userCredentials,
            HttpStatusCode::OK
        )->getBody()->getContents();
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

        return $this->sendGet(
            \sprintf(
                '/projection/%s/state?partition=%s',
                \urlencode($name),
                \urlencode($partition)
            ),
            $userCredentials,
            HttpStatusCode::OK
        )->getBody()->getContents();
    }

    /**
     * Returns String of JSON containing projection result
     */
    public function getResult(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->sendGet(
            \sprintf(
                '/projection/%s/result',
                \urlencode($name)
            ),
            $userCredentials,
            HttpStatusCode::OK
        )->getBody()->getContents();
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

        return $this->sendGet(
            \sprintf(
                '/projection/%s/result?partition=%s',
                \urlencode($name),
                \urlencode($partition)
            ),
            $userCredentials,
            HttpStatusCode::OK
        )->getBody()->getContents();
    }

    /**
     * Returns String of JSON containing projection statistics
     */
    public function getStatistics(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->sendGet(
            \sprintf(
                '/projection/%s/statistics',
                \urlencode($name)
            ),
            $userCredentials,
            HttpStatusCode::OK
        )->getBody()->getContents();
    }

    public function getQuery(string $name, ?UserCredentials $userCredentials = null): string
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        return $this->sendGet(
            \sprintf(
                '/projection/%s/query',
                \urlencode($name)
            ),
            $userCredentials,
            HttpStatusCode::OK
        )->getBody()->getContents();
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

        $this->sendPut(
            \sprintf(
                '/projection/%s/query?emit=' . (int) $emitEnabled,
                \urlencode($name)
            ),
            $query,
            $userCredentials,
            HttpStatusCode::OK
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

        $this->sendDelete(
            \sprintf(
                '/projection/%s?deleteEmittedStreams=%d',
                \urlencode($name),
                (int) $deleteEmittedStreams
            ),
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    public function reset(string $name, ?UserCredentials $userCredentials = null): void
    {
        if ('' === $name) {
            throw new InvalidArgumentException('Name is required');
        }

        $this->sendPost(
            \sprintf(
                '/projection/%s/command/reset',
                \urlencode($name)
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    private function buildProjectionDetails(array $entry): ProjectionDetails
    {
        return new ProjectionDetails(
            $entry['coreProcessingTime'],
            $entry['version'],
            $entry['epoch'],
            $entry['effectiveName'],
            $entry['writesInProgress'],
            $entry['readsInProgress'],
            $entry['partitionsCached'],
            $entry['status'],
            $entry['stateReason'],
            $entry['name'],
            $entry['mode'],
            $entry['position'],
            $entry['progress'],
            $entry['lastCheckpoint'],
            $entry['eventsProcessedAfterRestart'],
            $entry['statusUrl'],
            $entry['stateUrl'],
            $entry['resultUrl'],
            $entry['queryUrl'],
            $entry['enableCommandUrl'],
            $entry['disableCommandUrl'],
            $entry['checkpointStatus'],
            $entry['bufferedEvents'],
            $entry['writePendingEventsBeforeCheckpoint'],
            $entry['writePendingEventsAfterCheckpoint']
        );
    }

    private function sendGet(
        string $uri,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): ResponseInterface {
        $response = $this->httpClient->get(
            $uri,
            [],
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() !== $expectedCode) {
            throw new ProjectionCommandFailedException(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for GET on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $uri
                )
            );
        }

        return $response;
    }

    private function sendDelete(
        string $uri,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): void {
        $response = $this->httpClient->delete(
            $uri,
            [],
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() !== $expectedCode) {
            throw new ProjectionCommandFailedException(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for DELETE on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $uri
                )
            );
        }
    }

    private function sendPut(
        string $uri,
        string $content,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): void {
        $response = $this->httpClient->put(
            $uri,
            ['Content-Type' => 'application/json'],
            $content,
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() !== $expectedCode) {
            throw new ProjectionCommandFailedException(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for PUT on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $uri
                )
            );
        }
    }

    private function sendPost(
        string $uri,
        string $content,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): void {
        $response = $this->httpClient->post(
            $uri,
            ['Content-Type' => 'application/json'],
            $content,
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() === HttpStatusCode::CONFLICT) {
            throw new ProjectionCommandConflictException($response->getStatusCode(), $response->getReasonPhrase());
        }

        if ($response->getStatusCode() !== $expectedCode) {
            throw new ProjectionCommandFailedException(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for POST on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $uri
                )
            );
        }
    }
}
