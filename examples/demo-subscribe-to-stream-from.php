<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2019 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2018-2019 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStoreHttpClient;

use Prooph\EventStore\CatchUpSubscriptionDropped;
use Prooph\EventStore\CatchUpSubscriptionSettings;
use Prooph\EventStore\EventAppearedOnCatchupSubscription;
use Prooph\EventStore\EventStoreCatchUpSubscription;
use Prooph\EventStore\LiveProcessingStartedOnCatchUpSubscription;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SubscriptionDropReason;
use Throwable;

require __DIR__ . '/../vendor/autoload.php';

$connection = EventStoreConnectionFactory::create();

$subscription = $connection->subscribeToStreamFrom(
    'foo-bar',
    null,
    CatchUpSubscriptionSettings::default(),
    new class() implements EventAppearedOnCatchupSubscription {
        public function __invoke(
            EventStoreCatchUpSubscription $subscription,
            ResolvedEvent $resolvedEvent
        ): void {
            echo 'incoming event: ' . $resolvedEvent->originalEventNumber() . '@' . $resolvedEvent->originalStreamName() . PHP_EOL;
            echo 'data: ' . $resolvedEvent->originalEvent()->data() . PHP_EOL;
        }
    },
    new class() implements LiveProcessingStartedOnCatchUpSubscription {
        public function __invoke(EventStoreCatchUpSubscription $subscription): void
        {
            echo 'liveProcessingStarted on ' . $subscription->streamId() . PHP_EOL;
        }
    },
    new class() implements CatchUpSubscriptionDropped {
        public function __invoke(
            EventStoreCatchUpSubscription $subscription,
            SubscriptionDropReason $reason,
            ?Throwable $exception = null
        ): void {
            echo 'dropped with reason: ' . $reason->name() . PHP_EOL;

            if ($exception) {
                echo 'ex: ' . $exception->getMessage() . PHP_EOL;
            }
        }
    }
);

$subscription->start();
