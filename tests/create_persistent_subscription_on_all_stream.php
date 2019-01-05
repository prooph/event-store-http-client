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

namespace ProophTest\EventStoreHttpClient;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Exception\AccessDenied;
use Prooph\EventStore\PersistentSubscriptionSettings;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use Throwable;

class create_persistent_subscription_on_all_stream extends TestCase
{
    /** @test */
    public function the_completion_fails_with_invalid_stream(): void
    {
        $conn = TestConnection::create();

        $settings = PersistentSubscriptionSettings::create()
            ->doNotResolveLinkTos()
            ->startFromCurrent()
            ->build();

        try {
            $conn->createPersistentSubscription(
                '$all',
                'shitbird',
                $settings
            );

            $this->fail('Should have thrown');
        } catch (Throwable $e) {
            $this->assertInstanceOf(AccessDenied::class, $e);
        }
    }
}
