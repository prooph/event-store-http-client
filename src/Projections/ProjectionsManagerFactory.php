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

namespace Prooph\EventStoreHttpClient\Projections;

use Http\Discovery\HttpClientDiscovery;
use Http\Discovery\MessageFactoryDiscovery;
use Http\Message\RequestFactory;
use Prooph\EventStore\Projections\ProjectionsManager as SyncProjectionsManager;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Psr\Http\Client\ClientInterface;

class ProjectionsManagerFactory
{
    public static function create(
        ClientInterface $client = null,
        RequestFactory $requestFactory = null,
        ConnectionSettings $settings = null
    ): SyncProjectionsManager {
        return new ProjectionsManager(
            $client ?? HttpClientDiscovery::find(),
            $requestFactory ?? MessageFactoryDiscovery::find(),
            $settings ?? ConnectionSettings::default()
        );
    }
}
