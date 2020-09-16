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
use Prooph\EventStore\EventStorePersistentSubscription as EventStorePersistentSubscriptionInterface;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Internal\DropData;
use Prooph\EventStore\Internal\PersistentEventStoreSubscription;
use Prooph\EventStore\Internal\ResolvedEvent as InternalResolvedEvent;
use Prooph\EventStore\PersistentSubscriptionNakEventAction;
use Prooph\EventStore\PersistentSubscriptionResolvedEvent;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use ReflectionProperty;
use SplQueue;
use Throwable;

class EventStorePersistentSubscription implements EventStorePersistentSubscriptionInterface
{
    private HttpClient $httpClient;

    private ?ResolvedEvent $dropSubscriptionEvent = null;

    private string $subscriptionId;
    private string $streamId;
    private Closure $eventAppeared;
    private ?Closure $subscriptionDropped;
    private ?UserCredentials $userCredentials;
    private bool $autoAck;

    private PersistentEventStoreSubscription $subscription;
    private SplQueue $queue;
    private bool $isProcessing = false;
    private ?DropData $dropData = null;

    private bool $isDropped = false;
    private int $bufferSize;
    private bool $stopped = true;

    /**
     * @internal
     *
     * @param Closure(EventStorePersistentSubscription, ResolvedEvent, null|int): void $eventAppeared
     * @param null|Closure(EventStorePersistentSubscription, SubscriptionDropReason, null|Throwable): void $subscriptionDropped
     */
    public function __construct(
        HttpClient $httpClient,
        string $subscriptionId,
        string $streamId,
        Closure $eventAppeared,
        ?Closure $subscriptionDropped,
        ?UserCredentials $userCredentials,
        int $bufferSize = 10,
        bool $autoAck = true
    ) {
        if (null === $this->dropSubscriptionEvent) {
            $this->dropSubscriptionEvent = new ResolvedEvent(null, null, null);
        }

        $this->httpClient = $httpClient;
        $this->subscriptionId = $subscriptionId;
        $this->streamId = $streamId;
        $this->eventAppeared = $eventAppeared;
        $this->subscriptionDropped = $subscriptionDropped;
        $this->userCredentials = $userCredentials;
        $this->bufferSize = $bufferSize;
        $this->autoAck = $autoAck;
        $this->queue = new SplQueue();
    }

    /** @internal */
    public function startSubscription(
        string $subscriptionId,
        string $streamId,
        int $bufferSize,
        ?UserCredentials $userCredentials,
        callable $onEventAppeared,
        ?callable $onSubscriptionDropped
    ): PersistentEventStoreSubscription {
        $operation = new ConnectToPersistentSubscriptionOperation(
            $this->httpClient,
            $subscriptionId,
            $bufferSize,
            $streamId,
            $userCredentials,
            $onEventAppeared,
            $onSubscriptionDropped
        );

        return $operation->createSubscriptionObject();
    }

    public function start(): void
    {
        $this->stopped = false;

        $eventAppeared = function (
            PersistentEventStoreSubscription $subscription,
            PersistentSubscriptionResolvedEvent $resolvedEvent
        ): void {
            $this->onEventAppeared($resolvedEvent);
        };

        $subscriptionDropped = function (
            PersistentEventStoreSubscription $subscription,
            SubscriptionDropReason $reason,
            ?Throwable $exception
        ): void {
            $this->onSubscriptionDropped($reason, $exception);
        };

        /** @var PersistentEventStoreSubscription $subscription */
        $this->subscription = $subscription = $this->startSubscription(
            $this->subscriptionId,
            $this->streamId,
            $this->bufferSize,
            $this->userCredentials,
            $eventAppeared,
            $subscriptionDropped
        );

        // @todo dirty hack
        $property = new ReflectionProperty($subscription, 'subscriptionOperation');
        $property->setAccessible(true);

        $operation = $property->getValue($subscription);

        while (! $this->isDropped) {
            foreach ($operation->readFromSubscription($this->bufferSize) as $event) {
                $this->enqueue($event);
            }

            $this->processQueue();
        }
    }

    /**
     * Acknowledge that a message have completed processing (this will tell the server it has been processed)
     * Note: There is no need to ack a message if you have Auto Ack enabled
     *
     * @param InternalResolvedEvent $event
     *
     * @return void
     */
    public function acknowledge(InternalResolvedEvent $event): void
    {
        $this->subscription->notifyEventsProcessed([$event->originalEvent()->eventId()]);
    }

    /**
     * Acknowledge that a message have completed processing (this will tell the server it has been processed)
     * Note: There is no need to ack a message if you have Auto Ack enabled
     *
     * @param InternalResolvedEvent[] $events
     *
     * @return void
     */
    public function acknowledgeMultiple(array $events): void
    {
        $ids = \array_map(
            function (InternalResolvedEvent $event): EventId {
                return $event->originalEvent()->eventId();
            },
            $events
        );

        $this->subscription->notifyEventsProcessed($ids);
    }

    /**
     * Acknowledge that a message have completed processing (this will tell the server it has been processed)
     * Note: There is no need to ack a message if you have Auto Ack enabled
     *
     * @param EventId $eventId
     *
     * @return void
     */
    public function acknowledgeEventId(EventId $eventId): void
    {
        $this->subscription->notifyEventsProcessed([$eventId]);
    }

    /**
     * Acknowledge that a message have completed processing (this will tell the server it has been processed)
     * Note: There is no need to ack a message if you have Auto Ack enabled
     *
     * @param EventId[] $eventIds
     *
     * @return void
     */
    public function acknowledgeMultipleEventIds(array $eventIds): void
    {
        $this->subscription->notifyEventsProcessed($eventIds);
    }

    /**
     * Mark a message failed processing. The server will be take action based upon the action paramter
     */
    public function fail(
        InternalResolvedEvent $event,
        PersistentSubscriptionNakEventAction $action,
        string $reason
    ): void {
        $this->subscription->notifyEventsFailed([$event->originalEvent()->eventId()], $action, $reason);
    }

    /**
     * Mark n messages that have failed processing. The server will take action based upon the action parameter
     *
     * @param InternalResolvedEvent[] $events
     * @param PersistentSubscriptionNakEventAction $action
     * @param string $reason
     */
    public function failMultiple(
        array $events,
        PersistentSubscriptionNakEventAction $action,
        string $reason
    ): void {
        $ids = \array_map(
            function (InternalResolvedEvent $event): EventId {
                return $event->originalEvent()->eventId();
            },
            $events
        );

        $this->subscription->notifyEventsFailed($ids, $action, $reason);
    }

    public function failEventId(EventId $eventId, PersistentSubscriptionNakEventAction $action, string $reason): void
    {
        $this->subscription->notifyEventsFailed([$eventId], $action, $reason);
    }

    public function failMultipleEventIds(array $eventIds, PersistentSubscriptionNakEventAction $action, string $reason): void
    {
        foreach ($eventIds as $eventId) {
            \assert($eventId instanceof EventId);
        }

        $this->subscription->notifyEventsFailed($eventIds, $action, $reason);
    }

    public function stop(): void
    {
        $this->enqueueSubscriptionDropNotification(SubscriptionDropReason::userInitiated(), null);
    }

    private function enqueueSubscriptionDropNotification(
        SubscriptionDropReason $reason,
        ?Throwable $error
    ): void {
        // if drop data was already set -- no need to enqueue drop again, somebody did that already
        if (null === $this->dropData) {
            $this->dropData = new DropData($reason, $error);

            $this->enqueue(
                new PersistentSubscriptionResolvedEvent($this->dropSubscriptionEvent, null)
            );
        }
    }

    private function onSubscriptionDropped(
        SubscriptionDropReason $reason,
        ?Throwable $exception): void
    {
        $this->enqueueSubscriptionDropNotification($reason, $exception);
    }

    private function onEventAppeared(
        PersistentSubscriptionResolvedEvent $resolvedEvent
    ): void {
        $this->enqueue($resolvedEvent);
    }

    private function enqueue(PersistentSubscriptionResolvedEvent $resolvedEvent): void
    {
        $this->queue[] = $resolvedEvent;

        if (! $this->isProcessing) {
            $this->isProcessing = true;

            $this->processQueue();
        }
    }

    private function processQueue(): void
    {
        do {
            while (! $this->queue->isEmpty()) {
                $e = $this->queue->dequeue();
                \assert($e instanceof PersistentSubscriptionResolvedEvent);

                if ($e->event() === $this->dropSubscriptionEvent) {
                    // drop subscription artificial ResolvedEvent

                    if (null === $this->dropData) {
                        throw new RuntimeException('Drop reason not specified');
                    }

                    $this->dropSubscription($this->dropData->reason(), $this->dropData->error());

                    return;
                }

                if (null !== $this->dropData) {
                    $this->dropSubscription($this->dropData->reason(), $this->dropData->error());

                    return;
                }

                try {
                    ($this->eventAppeared)($this, $e->event(), $e->retryCount());

                    if ($this->autoAck) {
                        $this->subscription->notifyEventsProcessed([$e->originalEvent()->eventId()]);
                    }
                } catch (Throwable $ex) {
                    //TODO GFY should we autonak here?

                    $this->dropSubscription(SubscriptionDropReason::eventHandlerException(), $ex);

                    return;
                }
            }
        } while (! $this->queue->isEmpty() && $this->isProcessing);

        $this->isProcessing = false;
    }

    private function dropSubscription(SubscriptionDropReason $reason, ?Throwable $error): void
    {
        if (! $this->isDropped) {
            $this->isDropped = true;

            if (null !== $this->subscription) {
                $this->subscription->unsubscribe();
            }

            if ($this->subscriptionDropped) {
                ($this->subscriptionDropped)($this, $reason, $error);
            }

            $this->stopped = true;
        }
    }
}
