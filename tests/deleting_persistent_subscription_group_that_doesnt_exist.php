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
use Prooph\EventStore\Exception\InvalidOperationException;
use Prooph\EventStore\Util\Guid;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;

class deleting_persistent_subscription_group_that_doesnt_exist extends TestCase
{
    /** @test */
    public function the_delete_fails_with_argument_exception(): void
    {
        $conn = TestConnection::create();

        $this->expectException(InvalidOperationException::class);

        $conn->deletePersistentSubscription(
            Guid::generateAsHex(),
            Guid::generateAsHex(),
            DefaultData::adminCredentials()
        );
    }
}
