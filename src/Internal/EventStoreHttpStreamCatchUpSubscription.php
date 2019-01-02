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

use Prooph\EventStore\CatchUpSubscriptionDropped;
use Prooph\EventStore\CatchUpSubscriptionSettings;
use Prooph\EventStore\EventAppearedOnCatchupSubscription;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\EventStoreStreamCatchUpSubscription;
use Prooph\EventStore\Exception\OutOfRangeException;
use Prooph\EventStore\Exception\StreamDeleted;
use Prooph\EventStore\LiveProcessingStartedOnCatchUpSubscription;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SliceReadStatus;
use Prooph\EventStore\StreamEventsSlice;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use Throwable;

class EventStoreHttpStreamCatchUpSubscription extends EventStoreCatchUpSubscription implements EventStoreStreamCatchUpSubscription
{
    /** @var int */
    private $nextReadEventNumber;
    /** @var int */
    private $lastProcessedEventNumber;

    /**
     * @internal
     */
    public function __construct(
        EventStoreConnection $connection,
        string $streamId,
        ?int $fromEventNumberExclusive, // if null from the very beginning
        ?UserCredentials $userCredentials,
        EventAppearedOnCatchupSubscription $eventAppeared,
        ?LiveProcessingStartedOnCatchUpSubscription $liveProcessingStarted,
        ?CatchUpSubscriptionDropped $subscriptionDropped,
        CatchUpSubscriptionSettings $settings
    ) {
        parent::__construct(
            $connection,
            $streamId,
            $userCredentials,
            $eventAppeared,
            $liveProcessingStarted,
            $subscriptionDropped,
            $settings
        );

        $this->lastProcessedEventNumber = $fromEventNumberExclusive ?? -1;
        $this->nextReadEventNumber = $fromEventNumberExclusive ?? 0;
    }

    public function lastProcessedEventNumber(): int
    {
        return $this->lastProcessedEventNumber;
    }

    protected function readEventsTill(
        EventStoreConnection $connection,
        bool $resolveLinkTos,
        ?UserCredentials $userCredentials,
        ?int $lastCommitPosition,
        ?int $lastEventNumber
    ): void {
        $this->readEventsInternal($connection, $resolveLinkTos, $userCredentials, $lastEventNumber);
    }

    private function readEventsInternal(
        EventStoreConnection $connection,
        bool $resolveLinkTos,
        ?UserCredentials $userCredentials,
        ?int $lastEventNumber
    ): void {
        do {
            $slice = $connection->readStreamEventsForward(
                $this->streamId(),
                $this->nextReadEventNumber,
                $this->readBatchSize,
                $resolveLinkTos,
                $userCredentials
            );

            $shouldStopOrDone = $this->readEventsCallback($slice, $lastEventNumber);
        } while (! $shouldStopOrDone);
    }

    private function readEventsCallback(StreamEventsSlice $slice, ?int $lastEventNumber): bool
    {
        return $this->shouldStop || $this->processEvents($lastEventNumber, $slice);
    }

    private function processEvents(?int $lastEventNumber, StreamEventsSlice $slice): bool
    {
        switch ($slice->status()->value()) {
            case SliceReadStatus::SUCCESS:
                foreach ($slice->events() as $e) {
                    $this->tryProcess($e);
                }
                $this->nextReadEventNumber = $slice->nextEventNumber();
                $done = (null === $lastEventNumber) ? $slice->isEndOfStream() : $slice->nextEventNumber() > $lastEventNumber;

                break;
            case SliceReadStatus::STREAM_NOT_FOUND:
                if (null !== $lastEventNumber && $lastEventNumber !== -1) {
                    throw new \Exception(\sprintf(
                        'Impossible: stream %s disappeared in the middle of catching up subscription %s',
                        $this->streamId(),
                        $this->subscriptionName()
                    ));
                }

                $done = true;

                break;
            case SliceReadStatus::STREAM_DELETED:
                throw StreamDeleted::with($this->streamId());
            default:
                throw new OutOfRangeException(\sprintf(
                    'Unexpected SliceReadStatus "%s" received',
                    $slice->status()->name()
                ));
        }

        if (! $done && $slice->isEndOfStream()) {
            // we are waiting for server to flush its data
            \sleep(1);
        }

        return $done;
    }

    protected function tryProcess(ResolvedEvent $e): void
    {
        if ($e->originalEventNumber() > $this->lastProcessedEventNumber) {
            try {
                ($this->eventAppeared)($this, $e);
            } catch (Throwable $ex) {
                $this->dropSubscription(SubscriptionDropReason::eventHandlerException(), $ex);
            }

            $this->lastProcessedEventNumber = $e->originalEventNumber();
        }
    }
}
