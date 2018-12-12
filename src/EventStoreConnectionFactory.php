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
use Http\Discovery\UriFactoryDiscovery;
use Http\Message\RequestFactory;
use Http\Message\ResponseFactory;
use Http\Message\UriFactory;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Prooph\EventStoreHttpClient\Internal\EventStoreHttpConnection;
use Psr\Http\Client\ClientInterface;

class EventStoreConnectionFactory
{
    public static function create(
        ConnectionSettings $settings = null,
        ClientInterface $client = null,
        RequestFactory $requestFactory = null,
        ResponseFactory $responseFactory = null,
        UriFactory $uriFactory = null
    ): EventStoreConnection {
        if (null === $settings) {
            $settings = ConnectionSettings::default();
        }

        if (null === $client) {
            $client = HttpClientDiscovery::find();
        }

        if (null === $requestFactory) {
            $requestFactory = MessageFactoryDiscovery::find();
        }

        if (null === $responseFactory) {
            $responseFactory = MessageFactoryDiscovery::find();
        }

        if (null === $uriFactory) {
            $uriFactory = UriFactoryDiscovery::find();
        }

        return new EventStoreHttpConnection(
            new HttpClient($client, $requestFactory, $responseFactory),
            $requestFactory,
            $uriFactory,
            $settings ?? ConnectionSettings::default()
        );
    }
}
