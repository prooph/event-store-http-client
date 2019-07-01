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

namespace ProophTest\EventStoreHttpClient;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Exception\InvalidOperationException;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\Util\Guid;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;

class update_non_existing_persistent_subscription extends TestCase
{
    /** @test */
    public function the_completion_fails_with_not_found(): void
    {
        $conn = TestConnection::create();

        $stream = Guid::generateAsHex();
        $settings = PersistentSubscriptionSettings::create()
            ->doNotResolveLinkTos()
            ->startFromCurrent()
            ->build();

        $this->expectException(InvalidOperationException::class);

        $conn->updatePersistentSubscription(
            $stream,
            'existing',
            $settings,
            DefaultData::adminCredentials()
        );
    }
}
