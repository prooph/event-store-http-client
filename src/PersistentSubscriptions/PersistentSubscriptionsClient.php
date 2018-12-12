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

use Prooph\EventStoreHttpClient\EndPoint;
use Prooph\EventStoreHttpClient\Exception\EventStoreConnectionException;
use Prooph\EventStoreHttpClient\Exception\PersistentSubscriptionCommandFailedException;
use Prooph\EventStoreHttpClient\Http\EndpointExtensions;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Prooph\EventStoreHttpClient\Http\HttpStatusCode;
use Prooph\EventStoreHttpClient\UserCredentials;
use Prooph\EventStoreHttpClient\Util\Json;
use Throwable;

/** @internal */
class PersistentSubscriptionsClient
{
    /** @var HttpClient */
    private $client;

    public function __construct(HttpClient $client)
    {
        $this->client = $client;
    }

    public function describe(
        EndPoint $endPoint,
        string $stream,
        string $subscriptionName,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): PersistentSubscriptionDetails {
        $body = $this->sendGet(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                '/subscriptions/%s/%s/info',
                $stream,
                $subscriptionName
            ),
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($body);

        return PersistentSubscriptionDetails::fromArray($data);
    }

    /**
     * @param EndPoint $endPoint
     * @param null|string $stream
     * @param null|UserCredentials $userCredentials
     * @param string $httpSchema
     * @return PersistentSubscriptionDetails[]
     */
    public function list(
        EndPoint $endPoint,
        ?string $stream = null,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): array {
        $formatString = '/subscriptions';

        if (null !== $stream) {
            $formatString .= "/$stream";
        }

        $body = $this->sendGet(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                $formatString
            ),
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($body);

        $details = [];

        foreach ($data as $entry) {
            $details[] = PersistentSubscriptionDetails::fromArray($entry);
        }

        return $details;
    }

    public function replayParkedMessages(
        EndPoint $endPoint,
        string $stream,
        string $subscriptionName,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): void {
        $this->sendPost(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                '/subscriptions/%s/%s/replayParked',
                $stream,
                $subscriptionName
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    private function sendGet(string $url, ?UserCredentials $userCredentials, int $expectedCode): string
    {
        $response = $this->client->get(
            $url,
            [],
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() !== $expectedCode) {
            throw new PersistentSubscriptionCommandFailedException(
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
        $response = $this->client->post(
            $url,
            [],
            $content,
            'application/json',
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() !== $expectedCode) {
            throw new PersistentSubscriptionCommandFailedException(
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
