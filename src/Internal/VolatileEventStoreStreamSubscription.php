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

use Prooph\EventStore\EventAppearedOnSubscription;
use Prooph\EventStore\EventStoreSubscription;
use Prooph\EventStore\Exception\AccessDeniedException;
use Prooph\EventStore\Exception\ObjectDisposedException;
use Prooph\EventStore\Internal\Consts;
use Prooph\EventStore\SliceReadStatus;
use Prooph\EventStore\SubscriptionDropped;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use Throwable;

class VolatileEventStoreStreamSubscription extends EventStoreSubscription
{
    /** @var EventStoreHttpConnection */
    private $connection;
    /** @var EventAppearedOnSubscription */
    private $eventAppeared;
    /** @var SubscriptionDropped|null */
    private $subscriptionDropped;
    /** @var UserCredentials|null */
    private $userCredentials;
    /** @var bool */
    private $resolveLinkTos;
    /** @var bool */
    private $running = false;
    /** @var bool */
    private $disposed = false;

    /** @internal */
    public function __construct(
        EventStoreHttpConnection $connection,
        EventAppearedOnSubscription $eventAppeared,
        ?SubscriptionDropped $subscriptionDropped,
        string $streamId,
        ?int $lastEventNumber,
        bool $resolveLinkTos,
        ?UserCredentials $userCredentials
    ) {
        parent::__construct($streamId, -1, $lastEventNumber);

        $this->connection = $connection;
        $this->eventAppeared = $eventAppeared;
        $this->subscriptionDropped = $subscriptionDropped;
        $this->userCredentials = $userCredentials;
        $this->resolveLinkTos = $resolveLinkTos;
    }

    public function start(): void
    {
        if ($this->disposed) {
            throw new ObjectDisposedException('This volatile subscription was already stopped');
        }

        $this->running = true;

        $lastEventNumber = $this->lastEventNumber();
        $stream = $this->streamId();

        while ($this->running) {
            try {
                $streamEventsSlice = $this->connection->readStreamEventsForwardPolling(
                    $stream,
                    $lastEventNumber,
                    Consts::CATCH_UP_DEFAULT_READ_BATCH_SIZE,
                    $this->resolveLinkTos,
                    1,
                    $this->userCredentials
                );
            } catch (AccessDeniedException $e) {
                if ($this->subscriptionDropped) {
                    ($this->subscriptionDropped)(
                        $this,
                        SubscriptionDropReason::accessDenied(),
                        $e
                    );
                }

                $this->unsubscribe();

                return;
            } catch (Throwable $e) {
                if ($this->subscriptionDropped) {
                    ($this->subscriptionDropped)(
                        $this,
                        SubscriptionDropReason::serverError(),
                        $e
                    );
                }

                $this->unsubscribe();

                return;
            }

            if ($streamEventsSlice->status()->equals(SliceReadStatus::streamDeleted())) {
                if ($this->subscriptionDropped) {
                    ($this->subscriptionDropped)(
                        $this,
                        SubscriptionDropReason::subscribingError()
                    );
                }

                $this->unsubscribe();

                return;
            }

            foreach ($streamEventsSlice->events() as $event) {
                try {
                    ($this->eventAppeared)($this, $event);
                } catch (Throwable $e) {
                    if ($this->subscriptionDropped) {
                        ($this->subscriptionDropped)(
                            $this,
                            SubscriptionDropReason::eventHandlerException(),
                            $e
                        );

                        $this->unsubscribe();

                        return;
                    }
                }
            }

            $lastEventNumber = $streamEventsSlice->nextEventNumber();
        }
    }

    public function unsubscribe(): void
    {
        $this->running = false;
        $this->disposed = true;
    }
}
