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

use Closure;
use Prooph\EventStore\CatchUpSubscriptionDropped;
use Prooph\EventStore\CatchUpSubscriptionSettings;
use Prooph\EventStore\EventAppearedOnCatchupSubscription;
use Prooph\EventStore\EventAppearedOnSubscription;
use Prooph\EventStore\EventStoreCatchUpSubscription as SyncEventStoreCatchUpSubscription;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\EventStoreSubscription;
use Prooph\EventStore\Internal\DropData;
use Prooph\EventStore\LiveProcessingStartedOnCatchUpSubscription;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SubscriptionDropped;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use SplQueue;
use Throwable;

abstract class EventStoreCatchUpSubscription implements SyncEventStoreCatchUpSubscription
{
    /** @var ResolvedEvent */
    private static $dropSubscriptionEvent;

    /** @var bool */
    private $isSubscribedToAll;
    /** @var string */
    private $streamId;
    /** @var string */
    private $subscriptionName;

    /** @var EventStoreConnection */
    private $connection;
    /** @var bool */
    private $resolveLinkTos;
    /** @var UserCredentials|null */
    private $userCredentials;

    /** @var int */
    protected $readBatchSize;
    /** @var int */
    protected $maxPushQueueSize;

    /** @var EventAppearedOnCatchupSubscription */
    protected $eventAppeared;
    /** @var LiveProcessingStartedOnCatchUpSubscription|null */
    private $liveProcessingStarted;
    /** @var CatchUpSubscriptionDropped|null */
    private $subscriptionDropped;

    /** @var SplQueue<ResolvedEvent> */
    private $liveQueue;
    /** @var EventStoreSubscription|null */
    private $subscription;
    /** @var DropData|null */
    private $dropData;
    /** @var bool */
    private $allowProcessing;
    /** @var bool */
    private $isProcessing;
    /** @var bool */
    protected $shouldStop;
    /** @var bool */
    private $isDropped;
    /** @var bool */
    private $stopped;

    /** @internal */
    public function __construct(
        EventStoreConnection $connection,
        string $streamId,
        ?UserCredentials $userCredentials,
        EventAppearedOnCatchupSubscription $eventAppeared,
        ?LiveProcessingStartedOnCatchUpSubscription $liveProcessingStarted,
        ?CatchUpSubscriptionDropped $subscriptionDropped,
        CatchUpSubscriptionSettings $settings
    ) {
        if (null === self::$dropSubscriptionEvent) {
            self::$dropSubscriptionEvent = new ResolvedEvent(null, null, null);
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
        $this->runSubscription();
    }

    public function stop(): void
    {
        $this->shouldStop = true;
        $this->enqueueSubscriptionDropNotification(SubscriptionDropReason::userInitiated(), null);
    }

    /** @throws Throwable */
    private function runSubscription(): void
    {
        $this->loadHistoricalEvents();
    }

    /** @throws Throwable */
    private function loadHistoricalEvents(): void
    {
        $this->stopped = false;
        $this->allowProcessing = false;

        if (! $this->shouldStop) {
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
            $eventAppeared = new class(Closure::fromCallable([$this, 'enqueuePushedEvent'])) implements EventAppearedOnSubscription {
                private $callback;

                public function __construct(callable $callback)
                {
                    $this->callback = $callback;
                }

                public function __invoke(
                    EventStoreSubscription $subscription,
                    ResolvedEvent $resolvedEvent
                ): void {
                    ($this->callback)($subscription, $resolvedEvent);
                }
            };

            $subscriptionDropped = new class(Closure::fromCallable([$this, 'serverSubscriptionDropped'])) implements SubscriptionDropped {
                private $callback;

                public function __construct(callable $callback)
                {
                    $this->callback = $callback;
                }

                public function __invoke(
                    EventStoreSubscription $subscription,
                    SubscriptionDropReason $reason,
                    ?Throwable $exception = null
                ): void {
                    ($this->callback)($reason, $exception);
                }
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

        if ($this->liveProcessingStarted) {
            ($this->liveProcessingStarted)($this);
        }

        $this->allowProcessing = true;

        $this->ensureProcessingPushQueue();
    }

    private function enqueuePushedEvent(EventStoreSubscription $subscription, ResolvedEvent $e): void
    {
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

            $this->liveQueue->enqueue(self::$dropSubscriptionEvent);

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

                if ($e === self::$dropSubscriptionEvent) {
                    $this->dropData = $this->dropData ?? new DropData(SubscriptionDropReason::unknown(), new \Exception('Drop reason not specified'));
                    $this->dropSubscription($this->dropData->reason(), $this->dropData->error());

                    $this->isProcessing = false;

                    return null;
                }

                try {
                    $this->tryProcess($e);
                } catch (Throwable $ex) {
                    $this->dropSubscription(SubscriptionDropReason::eventHandlerException(), $ex);

                    return null;
                }
            }
        } while ($this->liveQueue->count() > 0);

        $this->isProcessing = false;
    }

    public function dropSubscription(SubscriptionDropReason $reason, ?Throwable $error): void
    {
        if (! $this->isDropped) {
            $this->isDropped = true;

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
