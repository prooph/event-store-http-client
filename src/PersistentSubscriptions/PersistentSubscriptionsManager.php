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

namespace Prooph\EventStoreHttpClient\PersistentSubscriptions;

use Prooph\EventStore\EndPoint;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\PersistentSubscriptions\PersistentSubscriptionDetails;
use Prooph\EventStore\PersistentSubscriptions\PersistentSubscriptionsManager as SyncPersistentSubscriptionsManager;
use Prooph\EventStore\Transport\Http\EndpointExtensions;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\Http\HttpClient;

class PersistentSubscriptionsManager implements SyncPersistentSubscriptionsManager
{
    /** @var PersistentSubscriptionsClient */
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
        $this->client = new PersistentSubscriptionsClient($client);
        $this->httpEndPoint = $endPoint;
        $this->httpSchema = $schema;
        $this->defaultUserCredentials = $defaultUserCredentials;
    }

    public function describe(
        string $stream,
        string $subscriptionName,
        ?UserCredentials $userCredentials = null
    ): PersistentSubscriptionDetails {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($subscriptionName)) {
            throw new InvalidArgumentException('Subscription name cannot be empty');
        }

        return $this->client->describe(
            $this->httpEndPoint,
            $stream,
            $subscriptionName,
            $userCredentials,
            $this->httpSchema
        );
    }

    public function replayParkedMessages(
        string $stream,
        string $subscriptionName,
        ?UserCredentials $userCredentials = null
    ): void {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($subscriptionName)) {
            throw new InvalidArgumentException('Subscription name cannot be empty');
        }

        $this->client->replayParkedMessages(
            $this->httpEndPoint,
            $stream,
            $subscriptionName,
            $userCredentials,
            $this->httpSchema
        );
    }

    /**
     * @param null|string $stream
     * @param null|UserCredentials $userCredentials
     * @return PersistentSubscriptionDetails[]
     */
    public function list(?string $stream = null, ?UserCredentials $userCredentials = null): array
    {
        if ('' === $stream) {
            $stream = null;
        }

        return $this->client->list(
            $this->httpEndPoint,
            $stream,
            $userCredentials,
            $this->httpSchema
        );
    }
}
