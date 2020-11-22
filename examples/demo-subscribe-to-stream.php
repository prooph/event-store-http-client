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

namespace Prooph\EventStoreHttpClient;

use Prooph\EventStore\EventStoreSubscription;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\Internal\VolatileEventStoreStreamSubscription;
use Throwable;

require __DIR__ . '/../vendor/autoload.php';

$connection = EventStoreConnectionFactory::create();

$subscription = $connection->subscribeToStream(
    'foo-bar',
    true,
    function(
        EventStoreSubscription $subscription,
        ResolvedEvent $resolvedEvent
    ): void {
        echo 'incoming event: ' . $resolvedEvent->originalEventNumber() . '@' . $resolvedEvent->originalStreamName() . PHP_EOL;
        echo 'data: ' . $resolvedEvent->originalEvent()->data() . PHP_EOL;
    },
    function(
        EventStoreSubscription $subscription,
        SubscriptionDropReason $reason,
        ?Throwable $exception = null
    ): void {
        echo 'dropped with reason: ' . $reason->name() . PHP_EOL;

        if ($exception) {
            echo 'ex: ' . $exception->getMessage() . PHP_EOL;
        }
    },
    new UserCredentials('admin', 'changeit')
);

\assert($subscription instanceof VolatileEventStoreStreamSubscription);
echo 'last event number: ' . $subscription->lastEventNumber() . PHP_EOL;

$subscription->start();
