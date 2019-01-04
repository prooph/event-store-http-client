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
use Prooph\EventStore\EndPoint;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Prooph\EventStoreHttpClient\ConnectionString;
use Prooph\EventStoreHttpClient\EventStoreConnectionFactory as Factory;
use Prooph\EventStoreHttpClient\Internal\EventStoreHttpConnection;

final class connection_factory_can extends TestCase
{
    /** @test */
    public function create_from_settings_and_discover_client_and_request_factory(): void
    {
        $conn = Factory::create(
            null,
            null,
            ConnectionSettings::default()
        );

        \assert($conn instanceof EventStoreHttpConnection);

        $this->assertEquals(
            new EndPoint('localhost', 2113),
            $conn->connectionSettings()->endPoint()
        );
    }

    /** @test */
    public function create_from_connection_string(): void
    {
        $conn = Factory::create(
            null,
            null,
            ConnectionString::getConnectionSettings(
                'endpoint=foo:21345'
            )
        );

        \assert($conn instanceof EventStoreHttpConnection);

        $this->assertEquals('foo', $conn->connectionSettings()->endPoint()->host());
        $this->assertEquals(21345, $conn->connectionSettings()->endPoint()->port());
    }
}
