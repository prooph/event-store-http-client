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

use Prooph\EventStore\AllEventsSlice;
use Prooph\EventStore\CatchUpSubscriptionDropped;
use Prooph\EventStore\CatchUpSubscriptionSettings;
use Prooph\EventStore\EventAppearedOnCatchupSubscription;
use Prooph\EventStore\EventStoreAllCatchUpSubscription;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\LiveProcessingStartedOnCatchUpSubscription;
use Prooph\EventStore\Position;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use Throwable;

class EventStoreAllCatchUpSubscription extends EventStoreCatchUpSubscription implements EventStoreAllCatchUpSubscription
{
    /** @var Position */
    private $nextReadPosition;
    /** @var Position */
    private $lastProcessedPosition;

    /**
     * @internal
     */
    public function __construct(
        EventStoreConnection $connection,
        ?Position $fromPositionExclusive, // if null from the very beginning
        ?UserCredentials $userCredentials,
        EventAppearedOnCatchupSubscription $eventAppeared,
        ?LiveProcessingStartedOnCatchUpSubscription $liveProcessingStarted,
        ?CatchUpSubscriptionDropped $subscriptionDropped,
        CatchUpSubscriptionSettings $settings
    ) {
        parent::__construct(
            $connection,
            '',
            $userCredentials,
            $eventAppeared,
            $liveProcessingStarted,
            $subscriptionDropped,
            $settings
        );

        $this->lastProcessedPosition = $fromPositionExclusive ?? Position::end();
        $this->nextReadPosition = $fromPositionExclusive ?? Position::start();
    }

    public function lastProcessedPosition(): Position
    {
        return $this->lastProcessedPosition;
    }

    protected function readEventsTill(
        EventStoreConnection $connection,
        bool $resolveLinkTos,
        ?UserCredentials $userCredentials,
        ?int $lastCommitPosition,
        ?int $lastEventNumber
    ): void {
        $this->readEventsInternal($connection, $resolveLinkTos, $userCredentials, $lastCommitPosition);
    }

    private function readEventsInternal(
        EventStoreConnection $connection,
        bool $resolveLinkTos,
        ?UserCredentials $userCredentials,
        ?int $lastCommitPosition
    ): void {
        do {
            $slice = $connection->readAllEventsForward(
                $this->nextReadPosition,
                $this->readBatchSize,
                $resolveLinkTos,
                $userCredentials
            );

            $shouldStopOrDone = $this->readEventsCallback($slice, $lastCommitPosition);
        } while (! $shouldStopOrDone);
    }

    private function readEventsCallback(AllEventsSlice $slice, ?int $lastCommitPosition): bool
    {
        return $this->shouldStop || $this->processEvents($lastCommitPosition, $slice);
    }

    private function processEvents(?int $lastCommitPosition, AllEventsSlice $slice): bool
    {
        foreach ($slice->events() as $e) {
            if (null === $e->originalPosition()) {
                throw new RuntimeException(\sprintf(
                    'Subscription %s event came up with no OriginalPosition',
                    $this->subscriptionName()
                ));
            }

            $this->tryProcess($e);
        }

        $this->nextReadPosition = $slice->nextPosition();

        $done = (null === $lastCommitPosition)
            ? $slice->isEndOfStream()
            : $slice->nextPosition()->greaterOrEquals(new Position($lastCommitPosition, $lastCommitPosition));

        if (! $done && $slice->isEndOfStream()) {
            // we are waiting for server to flush its data
            \sleep(1);
        }

        return $done;
    }

    protected function tryProcess(ResolvedEvent $e): void
    {
        if ($e->originalPosition()->greater($this->lastProcessedPosition)) {
            try {
                ($this->eventAppeared)($this, $e);
            } catch (Throwable $ex) {
                $this->dropSubscription(SubscriptionDropReason::eventHandlerException(), $ex);
            }

            $this->lastProcessedPosition = $e->originalPosition();
        }
    }
}
