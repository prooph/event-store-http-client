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

namespace ProophTest\EventStoreHttpClient;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\Util\Guid;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use Throwable;

class create_persistent_subscription_on_non_existing_stream extends TestCase
{
    /**
     * @test
     * @doesNotPerformAssertions
     * @throws Throwable
     */
    public function the_completion_succeeds(): void
    {
        $conn = TestConnection::create(DefaultData::adminCredentials());

        $stream = Guid::generateAsHex();
        $settings = PersistentSubscriptionSettings::create()
            ->doNotResolveLinkTos()
            ->startFromCurrent()
            ->build();

        $conn->createPersistentSubscription(
            $stream,
            'nonexistinggroup',
            $settings,
            DefaultData::adminCredentials()
        );
    }
}
