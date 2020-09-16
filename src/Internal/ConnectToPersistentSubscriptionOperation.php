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

namespace Prooph\EventStoreHttpClient\Internal;

use Closure;
use Prooph\EventStore\EventId;
use Prooph\EventStore\EventStoreSubscription;
use Prooph\EventStore\Exception\AccessDenied;
use Prooph\EventStore\Exception\EventStoreConnectionException;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Internal\ConnectToPersistentSubscriptions;
use Prooph\EventStore\Internal\PersistentEventStoreSubscription;
use Prooph\EventStore\PersistentSubscriptionNakEventAction;
use Prooph\EventStore\PersistentSubscriptionResolvedEvent;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use SplQueue;
use Throwable;

/** @internal */
class ConnectToPersistentSubscriptionOperation implements ConnectToPersistentSubscriptions
{
    private HttpClient $httpClient;
    private string $groupName;
    private int $bufferSize;
    private string $subscriptionId;
    protected string $streamId;
    protected bool $resolveLinkTos;
    protected ?UserCredentials $userCredentials;
    protected Closure $eventAppeared;
    private ?Closure $subscriptionDropped = null;
    private SplQueue $actionQueue;
    private ?EventStoreSubscription $subscription = null;
    private bool $unsubscribed = false;

    public function __construct(
        HttpClient $httpClient,
        string $groupName,
        int $bufferSize,
        string $streamId,
        ?UserCredentials $userCredentials,
        callable $eventAppeared,
        ?callable $subscriptionDropped
    ) {
        $this->httpClient = $httpClient;
        $this->groupName = $groupName;
        $this->bufferSize = $bufferSize;
        $this->streamId = $streamId;
        $this->resolveLinkTos = false;
        $this->userCredentials = $userCredentials;
        $this->eventAppeared = $eventAppeared;
        $this->subscriptionDropped = $subscriptionDropped;
        $this->actionQueue = new SplQueue();
    }

    public function readFromSubscription(int $amount): array
    {
        $response = $this->httpClient->get(
            \sprintf(
                '/subscriptions/%s/%s/%d?embed=tryharder',
                \urlencode($this->streamId),
                \urlencode($this->groupName),
                $amount
            ),
            [
                'Accept' => 'application/vnd.eventstore.competingatom+json',
            ],
            $this->userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDenied::toStream($this->streamId);
            case 404:
                throw new RuntimeException(\sprintf(
                    'Subscription with stream \'%s\' and group name \'%s\' not found',
                    $this->streamId,
                    $this->groupName
                ));
            case 200:
                $json = \json_decode($response->getBody()->getContents(), true);

                $events = [];

                if (null === $json) {
                    return $events;
                }

                if (empty($json['entries'])) {
                    \sleep(1); // @todo make configurable?

                    return $events;
                }

                foreach (\array_reverse($json['entries']) as $entry) {
                    $events[] = new PersistentSubscriptionResolvedEvent(
                        ResolvedEventParser::parse($entry),
                        null
                    );
                }

                return $events;
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
    }

    public function createSubscriptionObject(): PersistentEventStoreSubscription
    {
        return new PersistentEventStoreSubscription(
            $this,
            $this->streamId,
            0,
            null
        );
    }

    /** @param EventId[] $eventIds */
    public function notifyEventsProcessed(array $eventIds): void
    {
        if (empty($eventIds)) {
            throw new InvalidArgumentException('EventIds cannot be empty');
        }

        $eventIds = \array_map(function (EventId $eventId): string {
            return $eventId->toString();
        }, $eventIds);

        $response = $this->httpClient->post(
            \sprintf(
                '/subscriptions/%s/%s/ack?ids=%s',
                \urlencode($this->streamId),
                \urlencode($this->groupName),
                \implode(',', $eventIds)
            ),
            [
                'Content-Length' => 0,
            ],
            '',
            $this->userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        switch ($response->getStatusCode()) {
            case 202:
                return;
            case 401:
                throw AccessDenied::toStream($this->streamId);
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
    }

    /**
     * @param EventId[] $eventIds
     * @param PersistentSubscriptionNakEventAction $action
     * @param string $reason
     */
    public function notifyEventsFailed(
        array $eventIds,
        PersistentSubscriptionNakEventAction $action,
        string $reason
    ): void {
        if (empty($eventIds)) {
            throw new InvalidArgumentException('EventIds cannot be empty');
        }

        $eventIds = \array_map(function (EventId $eventId): string {
            return $eventId->toString();
        }, $eventIds);

        $response = $this->httpClient->post(
            \sprintf(
                '/subscriptions/%s/%s/nack?ids=%s&action=%s',
                \urlencode($this->streamId),
                \urlencode($this->groupName),
                \implode(',', $eventIds),
                $action->name()
            ),
            [
                'Content-Length' => 0,
            ],
            '',
            $this->userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        switch ($response->getStatusCode()) {
            case 202:
                return;
            case 401:
                throw AccessDenied::toStream($this->streamId);
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
    }

    public function unsubscribe(): void
    {
        $this->dropSubscription(SubscriptionDropReason::userInitiated(), null);
    }

    public function dropSubscription(
        SubscriptionDropReason $reason,
        ?Throwable $exception = null
    ): void {
        if (! $this->unsubscribed) {
            $this->unsubscribed = true;

            if (! $reason->equals(SubscriptionDropReason::userInitiated())) {
                $exception = $exception ?? new RuntimeException('Subscription dropped for ' . $reason);

                throw $exception;
            }

            if ($reason->equals(SubscriptionDropReason::userInitiated())
                && null !== $this->subscription
            ) {
                return;
            }

            if (null !== $this->subscription
                && $this->subscriptionDropped
            ) {
                ($this->subscriptionDropped)($this->subscription, $reason, $exception);
            }
        }
    }

    public function name(): string
    {
        return 'ConnectToPersistentSubscription';
    }

    public function __toString(): string
    {
        return \sprintf(
            'StreamId: %s, ResolveLinkTos: %s, GroupName: %s, BufferSize: %d, SubscriptionId: %s',
            $this->streamId,
            $this->resolveLinkTos ? 'yes' : 'no',
            $this->groupName,
            $this->bufferSize,
            $this->subscriptionId
        );
    }
}
