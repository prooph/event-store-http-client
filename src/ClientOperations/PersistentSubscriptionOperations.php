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

namespace Prooph\EventStoreHttpClient\ClientOperations;

use Http\Client\HttpClient;
use Http\Message\RequestFactory;
use Http\Message\UriFactory;
use Prooph\EventStore\Data\EventId;
use Prooph\EventStore\Data\EventRecord;
use Prooph\EventStore\Data\PersistentSubscriptionNakEventAction;
use Prooph\EventStore\Internal\PersistentSubscriptionOperations as BasePersistentSubscriptionOperations;

Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStoreHttpClient\Http\Method;
use Prooph\EventStoreHttpClient\UserCredentials;

/** @internal */
final class PersistentSubscriptionOperations extends Operation implements BasePersistentSubscriptionOperations
{
    /** @var HttpClient */
    private $httpClient;
    /** @var RequestFactory */
    private $requestFactory;
    /** @var UriFactory */
    private $uriFactory;
    /** @var string */
    private $baseUri;
    /** @var string */
    private $stream;
    /** @var string */
    private $groupName;
    /** @var ?UserCredentials */
    private $userCredentials;

    public function __construct(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        string $groupName,
        ?UserCredentials $userCredentials
    ) {
        $this->httpClient = $httpClient;
        $this->requestFactory = $requestFactory;
        $this->uriFactory = $uriFactory;
        $this->baseUri = $baseUri;
        $this->stream = $stream;
        $this->groupName = $groupName;
        $this->userCredentials = $userCredentials;
    }

    /**
     * @param int $amount
     * @return EventRecord[]
     */
    public function readFromSubscription(int $amount): array
    {
        return (new ReadFromSubscriptionOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $this->stream,
            $this->groupName,
            $amount,
            $this->userCredentials
        );
    }

    public function acknowledge(array $eventIds): void
    {
        $eventIds = \array_map(function (EventId $eventId): string {
            return $eventId->toString();
        }, $eventIds);

        $request = $this->requestFactory->createRequest(
            Method::Post,
            $this->uriFactory->createUri(\sprintf(
                '%s/subscriptions/%s/%s/ack?ids=%s',
                $this->baseUri,
                \urlencode($this->stream),
                \urlencode($this->groupName),
                \implode(',', $eventIds)
            )),
            [
                'Content-Length' => 0,
            ],
            ''
        );

        $response = $this->sendRequest($this->httpClient, $this->userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 202:
                return;
            case 401:
                throw AccessDeniedException::toStream($this->stream);
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }

    public function fail(array $eventIds, PersistentSubscriptionNakEventAction $action): void
    {
        $eventIds = \array_map(function (EventId $eventId): string {
            return $eventId->toString();
        }, $eventIds);

        $request = $this->requestFactory->createRequest(
            Method::Post,
            $this->uriFactory->createUri(\sprintf(
                '%s/subscriptions/%s/%s/nack?ids=%s&action=%s',
                $this->baseUri,
                \urlencode($this->stream),
                \urlencode($this->groupName),
                \implode(',', $eventIds),
                $action->name()
            )),
            [
                'Content-Length' => 0,
            ],
            ''
        );

        $response = $this->sendRequest($this->httpClient, $this->userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 202:
                return;
            case 401:
                throw AccessDeniedException::toStream($this->stream);
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
