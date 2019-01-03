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

namespace Prooph\EventStoreHttpClient\Internal;

use Prooph\EventStore\EventAppearedOnPersistentSubscription;
use Prooph\EventStore\EventId;
use Prooph\EventStore\EventStorePersistentSubscription;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Internal\DropData;
use Prooph\EventStore\Internal\PersistentEventStoreSubscription;
use Prooph\EventStore\Internal\ResolvedEvent as InternalResolvedEvent;
use Prooph\EventStore\PersistentSubscriptionDropped;
use Prooph\EventStore\PersistentSubscriptionNakEventAction;
use Prooph\EventStore\PersistentSubscriptionResolvedEvent;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use SplQueue;
use Throwable;

class EventStorePersistentHttpSubscription implements EventStorePersistentSubscription
{
    /** @var HttpClient */
    private $httpClient;

    /** @var ResolvedEvent */
    private static $dropSubscriptionEvent;

    /** @var string */
    private $subscriptionId;
    /** @var string */
    private $streamId;
    /** @var callable */
    private $eventAppeared;
    /** @var callable|null */
    private $subscriptionDropped;
    /** @var UserCredentials|null */
    private $userCredentials;
    /** @var bool */
    private $autoAck;

    /** @var PersistentEventStoreHttpSubscription */
    private $subscription;
    /** @var SplQueue */
    private $queue;
    /** @var bool */
    private $isProcessing = false;
    /** @var DropData */
    private $dropData;

    /** @var bool */
    private $isDropped = false;
    /** @var int */
    private $bufferSize;
    /** @var bool */
    private $stopped = true;

    /** @internal  */
    public function __construct(
        HttpClient $httpClient,
        string $subscriptionId,
        string $streamId,
        EventAppearedOnPersistentSubscription $eventAppeared,
        ?PersistentSubscriptionDropped $subscriptionDropped,
        ?UserCredentials $userCredentials,
        int $bufferSize = 10,
        bool $autoAck = true
    ) {
        if (null === self::$dropSubscriptionEvent) {
            self::$dropSubscriptionEvent = new ResolvedEvent(null, null, null);
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

        /** @var PersistentEventStoreHttpSubscription $subscription */
        $this->subscription = $subscription = $this->startSubscription(
            $this->subscriptionId,
            $this->streamId,
            $this->bufferSize,
            $this->userCredentials,
            $eventAppeared,
            $subscriptionDropped
        );

        $operation = $subscription->operation();

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
                new PersistentSubscriptionResolvedEvent(self::$dropSubscriptionEvent, null)
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

                if ($e->event() === self::$dropSubscriptionEvent) {
                    // drop subscription artificial ResolvedEvent

                    if (null === $this->dropData) {
                        throw new RuntimeException('Drop reason not specified');
                    }

                    $this->dropSubscription($this->dropData->reason(), $this->dropData->error());

                    return null;
                }

                if (null !== $this->dropData) {
                    $this->dropSubscription($this->dropData->reason(), $this->dropData->error());

                    return null;
                }

                try {
                    ($this->eventAppeared)($this, $e->event(), $e->retryCount());

                    if ($this->autoAck) {
                        $this->subscription->notifyEventsProcessed([$e->originalEvent()->eventId()]);
                    }
                } catch (Throwable $ex) {
                    //TODO GFY should we autonak here?

                    $this->dropSubscription(SubscriptionDropReason::eventHandlerException(), $ex);

                    return null;
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
