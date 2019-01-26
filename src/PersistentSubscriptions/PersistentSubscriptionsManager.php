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

namespace Prooph\EventStoreHttpClient\PersistentSubscriptions;

use Http\Message\RequestFactory;
use Prooph\EventStore\Exception\EventStoreConnectionException;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\PersistentSubscriptions\PersistentSubscriptionDetails;
use Prooph\EventStore\PersistentSubscriptions\PersistentSubscriptionsManager as SyncPersistentSubscriptionsManager;
use Prooph\EventStore\Transport\Http\HttpStatusCode;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStore\Util\Json;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Prooph\EventStoreHttpClient\Exception\PersistentSubscriptionCommandFailed;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Psr\Http\Client\ClientInterface;
use Throwable;

class PersistentSubscriptionsManager implements SyncPersistentSubscriptionsManager
{
    /** @var ConnectionSettings */
    private $settings;
    /** @var HttpClient */
    private $httpClient;

    /** @internal */
    public function __construct(
        ConnectionSettings $settings,
        ClientInterface $client,
        RequestFactory $requestFactory
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

        $body = $this->sendGet(
            \sprintf(
                '/subscriptions/%s/%s/info',
                \urlencode($stream),
                \urlencode($subscriptionName)
            ),
            $userCredentials,
            HttpStatusCode::OK
        );

        return PersistentSubscriptionDetails::fromArray(Json::decode($body));
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

        $this->sendPost(
            \sprintf(
                '/subscriptions/%s/%s/replayParked',
                \urlencode($stream),
                \urlencode($subscriptionName)
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
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

        $uri = '/subscriptions';

        if (null !== $stream) {
            $uri .= "/$stream";
        }

        $body = $this->sendGet(
            $uri,
            $userCredentials,
            HttpStatusCode::OK
        );

        $details = [];

        foreach (Json::decode($body) as $entry) {
            $details[] = PersistentSubscriptionDetails::fromArray($entry);
        }

        return $details;
    }

    private function sendGet(string $url, ?UserCredentials $userCredentials, int $expectedCode): string
    {
        $response = $this->httpClient->get(
            $url,
            [],
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() !== $expectedCode) {
            throw new PersistentSubscriptionCommandFailed(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for GET on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $url
                )
            );
        }

        return $response->getBody()->getContents();
    }

    private function sendPost(
        string $url,
        string $content,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): void {
        $response = $this->httpClient->post(
            $url,
            ['Content-Type' => 'application/json'],
            $content,
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() !== $expectedCode) {
            throw new PersistentSubscriptionCommandFailed(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for POST on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $url
                )
            );
        }
    }
}
