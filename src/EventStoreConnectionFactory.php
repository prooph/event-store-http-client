<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2018-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStoreHttpClient;

use Http\Discovery\HttpClientDiscovery;
use Http\Discovery\MessageFactoryDiscovery;
use Http\Message\RequestFactory;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStoreHttpClient\Internal\EventStoreHttpConnection;
use Psr\Http\Client\ClientInterface;

class EventStoreConnectionFactory
{
    public static function create(
        ClientInterface $client = null,
        RequestFactory $requestFactory = null,
        ConnectionSettings $settings = null
    ): EventStoreConnection {
        return new EventStoreHttpConnection(
            $client ?? HttpClientDiscovery::find(),
            $requestFactory ?? MessageFactoryDiscovery::find(),
            $settings ?? ConnectionSettings::default()
        );
    }
}
