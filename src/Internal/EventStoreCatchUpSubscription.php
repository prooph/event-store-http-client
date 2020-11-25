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
use Prooph\EventStore\CatchUpSubscriptionSettings;
use Prooph\EventStore\EventStoreCatchUpSubscription as SyncEventStoreCatchUpSubscription;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\EventStoreSubscription;
use Prooph\EventStore\Internal\DropData;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use Psr\Log\LoggerInterface as Logger;
use SplQueue;
use Throwable;

abstract class EventStoreCatchUpSubscription implements SyncEventStoreCatchUpSubscription
{
    private ?ResolvedEvent $dropSubscriptionEvent = null;

    protected Logger $log;

    private bool $isSubscribedToAll;
    private string $streamId;
    private string $subscriptionName;

    private EventStoreConnection $connection;
    private bool $resolveLinkTos;
    private ?UserCredentials $userCredentials;

    protected int $readBatchSize;
    protected int $maxPushQueueSize;

    /** @var Closure(EventStoreCatchUpSubscription, ResolvedEvent): void */
    protected Closure $eventAppeared;
    /** @var null|Closure(EventStoreCatchUpSubscription): void */
    private ?Closure $liveProcessingStarted;
    /** @var null|Closure(EventStoreCatchUpSubscription, SubscriptionDropReason, null|Throwable) */
    private ?Closure $subscriptionDropped;

    /** @var SplQueue<ResolvedEvent> */
    private SplQueue $liveQueue;
    private ?EventStoreSubscription $subscription;
    protected bool $verbose;
    private ?DropData $dropData;
    private bool $allowProcessing;
    private bool $isProcessing;
    protected bool $shouldStop = false;
    private bool $isDropped = false;
    private bool $stopped = true;

    /**
     * @internal
     *
     * @param Closure(EventStoreCatchUpSubscription, ResolvedEvent): void $eventAppeared
     * @param null|Closure(EventStoreCatchUpSubscription): void $liveProcessingStarted
     * @param null|Closure(EventStoreCatchUpSubscription, SubscriptionDropReason, null|Throwable): void $subscriptionDropped
     */
    public function __construct(
        EventStoreConnection $connection,
        Logger $logger,
        string $streamId,
        ?UserCredentials $userCredentials,
        Closure $eventAppeared,
        ?Closure $liveProcessingStarted,
        ?Closure $subscriptionDropped,
        CatchUpSubscriptionSettings $settings
    ) {
        if (null === $this->dropSubscriptionEvent) {
            $this->dropSubscriptionEvent = new ResolvedEvent(null, null, null);
        }

        $this->connection = $connection;
        $this->isSubscribedToAll = empty($streamId);
        $this->streamId = $streamId;
        $this->userCredentials = $userCredentials;
        $this->eventAppeared = $eventAppeared;
        $this->liveProcessingStarted = $liveProcessingStarted;
        $this->subscriptionDropped = $subscriptionDropped;
        $this->resolveLinkTos = $settings->resolveLinkTos();
        $this->readBatchSize = $settings->readBatchSize();
        $this->maxPushQueueSize = $settings->maxLiveQueueSize();
        $this->verbose = $settings->verboseLogging();
        $this->liveQueue = new SplQueue();
        $this->subscriptionName = $settings->subscriptionName() ?? '';
        $this->stopped = true;
        $this->subscription = null;
    }

    public function isSubscribedToAll(): bool
    {
        return $this->isSubscribedToAll;
    }

    public function streamId(): string
    {
        return $this->streamId;
    }

    public function subscriptionName(): string
    {
        return $this->subscriptionName;
    }

    abstract protected function readEventsTill(
        EventStoreConnection $connection,
        bool $resolveLinkTos,
        ?UserCredentials $userCredentials,
        ?int $lastCommitPosition,
        ?int $lastEventNumber
    ): void;

    abstract protected function tryProcess(ResolvedEvent $e): void;

    /**
     * @throws Throwable
     */
    public function start(): void
    {
        if ($this->verbose) {
            $this->log->debug(\sprintf(
                'Catch-up Subscription %s to %s: starting...',
                $this->subscriptionName,
                $this->isSubscribedToAll ? '<all>' : $this->streamId
            ));
        }

        $this->runSubscription();
    }

    public function stop(): void
    {
        if ($this->verbose) {
            $this->log->debug(\sprintf(
                'Catch-up Subscription %s to %s: requesting stop...',
                $this->subscriptionName,
                $this->isSubscribedToAll ? '<all>' : $this->streamId
            ));
        }

        $this->shouldStop = true;
        $this->enqueueSubscriptionDropNotification(SubscriptionDropReason::userInitiated(), null);

        if ($this->verbose) {
            $this->log->debug(\sprintf(
                'Waiting on subscription %s to stop',
                $this->subscriptionName
            ));
        }
    }

    /** @throws Throwable */
    private function runSubscription(): void
    {
        $this->loadHistoricalEvents();
    }

    /** @throws Throwable */
    private function loadHistoricalEvents(): void
    {
        if ($this->verbose) {
            $this->log->debug(\sprintf(
                'Catch-up Subscription %s to %s: running...',
                $this->subscriptionName,
                $this->isSubscribedToAll ? '<all>' : $this->streamId
            ));
        }

        $this->stopped = false;
        $this->allowProcessing = false;

        if (! $this->shouldStop) {
            if ($this->verbose) {
                $this->log->debug(\sprintf(
                    'Catch-up Subscription %s to %s: pulling events...',
                    $this->subscriptionName,
                    $this->isSubscribedToAll ? '<all>' : $this->streamId
                ));
            }

            try {
                $this->readEventsTill($this->connection, $this->resolveLinkTos, $this->userCredentials, null, null);
                $this->subscribeToStream();
            } catch (Throwable $ex) {
                $this->dropSubscription(SubscriptionDropReason::catchUpError(), $ex);

                throw $ex;
            }
        } else {
            $this->dropSubscription(SubscriptionDropReason::userInitiated(), null);
        }
    }

    private function subscribeToStream(): void
    {
        if (! $this->shouldStop) {
            if ($this->verbose) {
                $this->log->debug(\sprintf(
                    'Catch-up Subscription %s to %s: subscribing...',
                    $this->subscriptionName,
                    $this->isSubscribedToAll ? '<all>' : $this->streamId
                ));
            }

            $eventAppeared = function (
                EventStoreSubscription $subscription,
                ResolvedEvent $resolvedEvent
            ): void {
                $this->enqueuePushedEvent($subscription, $resolvedEvent);
            };

            $subscriptionDropped = function (
                EventStoreSubscription $subscription,
                SubscriptionDropReason $reason,
                ?Throwable $exception = null
            ): void {
                $this->serverSubscriptionDropped($reason, $exception);
            };

            $subscription = empty($this->streamId)
                ? $this->connection->subscribeToAll(
                    $this->resolveLinkTos,
                    $eventAppeared,
                    $subscriptionDropped,
                    $this->userCredentials
                )
                : $this->connection->subscribeToStream(
                    $this->streamId,
                    $this->resolveLinkTos,
                    $eventAppeared,
                    $subscriptionDropped,
                    $this->userCredentials
                );

            $this->subscription = $subscription;

            $this->readMissedHistoricEvents();

            $this->subscription->start();
        } else {
            $this->dropSubscription(SubscriptionDropReason::userInitiated(), null);
        }
    }

    private function readMissedHistoricEvents(): void
    {
        if (! $this->shouldStop) {
            if ($this->verbose) {
                $this->log->debug(\sprintf(
                    'Catch-up Subscription %s to %s: pulling events (if left)...',
                    $this->subscriptionName,
                    $this->isSubscribedToAll ? '<all>' : $this->streamId
                ));
            }

            $this->readEventsTill(
                $this->connection,
                $this->resolveLinkTos,
                $this->userCredentials,
                $this->subscription->lastCommitPosition(),
                $this->subscription->lastEventNumber()
            );
            $this->startLiveProcessing();
        } else {
            $this->dropSubscription(SubscriptionDropReason::userInitiated(), null);
        }
    }

    private function startLiveProcessing(): void
    {
        if ($this->shouldStop) {
            $this->dropSubscription(SubscriptionDropReason::userInitiated(), null);

            return;
        }

        if ($this->verbose) {
            $this->log->debug(\sprintf(
                'Catch-up Subscription %s to %s: processing live events...',
                $this->subscriptionName,
                $this->isSubscribedToAll ? '<all>' : $this->streamId
            ));
        }

        if ($this->liveProcessingStarted) {
            ($this->liveProcessingStarted)($this);
        }

        $this->allowProcessing = true;

        $this->ensureProcessingPushQueue();
    }

    private function enqueuePushedEvent(EventStoreSubscription $subscription, ResolvedEvent $e): void
    {
        if ($this->verbose) {
            /** @psalm-suppress PossiblyNullReference */
            $this->log->debug(\sprintf(
                'Catch-up Subscription %s to %s: event appeared (%s, %s, %s, @ %s)',
                $this->subscriptionName,
                $this->isSubscribedToAll ? '<all>' : $this->streamId,
                $e->originalStreamName(),
                $e->originalEventNumber(),
                $e->originalEvent()->eventType(),
                (string) $e->originalPosition()
            ));
        }

        if ($this->liveQueue->count() >= $this->maxPushQueueSize) {
            $this->enqueueSubscriptionDropNotification(SubscriptionDropReason::processingQueueOverflow(), null);
            $subscription->unsubscribe();

            return;
        }

        $this->liveQueue->enqueue($e);

        if ($this->allowProcessing) {
            $this->ensureProcessingPushQueue();
        }
    }

    private function serverSubscriptionDropped(
        SubscriptionDropReason $reason,
        ?Throwable $exception): void
    {
        $this->enqueueSubscriptionDropNotification($reason, $exception);
    }

    private function enqueueSubscriptionDropNotification(SubscriptionDropReason $reason, ?Throwable $error): void
    {
        // if drop data was already set -- no need to enqueue drop again, somebody did that already
        $dropData = new DropData($reason, $error);

        if (null === $this->dropData) {
            $this->dropData = $dropData;

            $this->liveQueue->enqueue($this->dropSubscriptionEvent);

            if ($this->allowProcessing) {
                $this->ensureProcessingPushQueue();
            }
        }
    }

    private function ensureProcessingPushQueue(): void
    {
        if (! $this->isProcessing) {
            $this->isProcessing = true;

            $this->processLiveQueue();
        }
    }

    private function processLiveQueue(): void
    {
        do {
            while (! $this->liveQueue->isEmpty()) {
                $e = $this->liveQueue->dequeue();
                \assert($e instanceof ResolvedEvent);

                if ($e === $this->dropSubscriptionEvent) {
                    $this->dropData = $this->dropData ?? new DropData(SubscriptionDropReason::unknown(), new \Exception('Drop reason not specified'));
                    $this->dropSubscription($this->dropData->reason(), $this->dropData->error());

                    $this->isProcessing = false;

                    return;
                }

                try {
                    $this->tryProcess($e);
                } catch (Throwable $ex) {
                    $this->dropSubscription(SubscriptionDropReason::eventHandlerException(), $ex);

                    return;
                }
            }
        } while ($this->liveQueue->count() > 0);

        $this->isProcessing = false;
    }

    public function dropSubscription(SubscriptionDropReason $reason, ?Throwable $error): void
    {
        if (! $this->isDropped) {
            $this->isDropped = true;

            if ($this->verbose) {
                $this->log->debug(\sprintf(
                    'Catch-up Subscription %s to %s: dropped subscription, reason: %s %s',
                    $this->subscriptionName,
                    $this->isSubscribedToAll ? '<all>' : $this->streamId,
                    $reason->name(),
                    null === $error ? '' : $error->getMessage()
                ));
            }

            if ($this->subscription) {
                $this->subscription->unsubscribe();
            }

            if ($this->subscriptionDropped) {
                ($this->subscriptionDropped)($this, $reason, $error);
            }

            $this->stopped = true;
        }
    }
}
