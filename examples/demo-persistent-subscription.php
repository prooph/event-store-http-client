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

use Prooph\EventStore\EventAppearedOnPersistentSubscription;
use Prooph\EventStore\EventStorePersistentSubscription;
use Prooph\EventStore\Exception\InvalidOperationException;
use Prooph\EventStore\Internal\PersistentSubscriptionCreateResult;
use Prooph\EventStore\PersistentSubscriptionDropped;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SubscriptionDropReason;
use Prooph\EventStore\UserCredentials;
use Throwable;

require __DIR__ . '/../vendor/autoload.php';

$connection = EventStoreConnectionFactory::create();

try {
    $result = $connection->deletePersistentSubscription(
        'foo-bar',
        'test-persistent-subscription',
        new UserCredentials('admin', 'changeit')
    );
    \var_dump($result);
} catch (InvalidOperationException $exception) {
    echo 'no such subscription exists (yet)' . PHP_EOL;
}

$result = $connection->createPersistentSubscription(
    'foo-bar',
    'test-persistent-subscription',
    PersistentSubscriptionSettings::default(),
    new UserCredentials('admin', 'changeit')
);
\assert($result instanceof PersistentSubscriptionCreateResult);

\var_dump($result);

$connection->connectToPersistentSubscription(
    'foo-bar',
    'test-persistent-subscription',
    new class() implements EventAppearedOnPersistentSubscription {
        public function __invoke(
            EventStorePersistentSubscription $subscription,
            ResolvedEvent $resolvedEvent,
            ?int $retryCount = null
        ): void {
            echo 'incoming event: ' . $resolvedEvent->originalEventNumber() . '@' . $resolvedEvent->originalStreamName() . PHP_EOL;
            echo 'data: ' . $resolvedEvent->originalEvent()->data() . PHP_EOL;
        }
    },
    new class() implements PersistentSubscriptionDropped {
        public function __invoke(
            EventStorePersistentSubscription $subscription,
            SubscriptionDropReason $reason,
            ?Throwable $exception = null
        ): void {
            echo 'dropped with reason: ' . $reason->name() . PHP_EOL;

            if ($exception) {
                echo 'ex: ' . $exception->getMessage() . PHP_EOL;
            }
        }
    },
    10,
    true,
    new UserCredentials('admin', 'changeit')
);
