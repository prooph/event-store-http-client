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

namespace Prooph\EventStoreHttpClient\UserManagement;

use Http\Discovery\HttpClientDiscovery;
use Http\Discovery\MessageFactoryDiscovery;
use Http\Message\RequestFactory;
use Prooph\EventStore\UserManagement\UsersManager as SyncUsersManager;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Psr\Http\Client\ClientInterface;

class UsersManagerFactory
{
    public static function create(
        ConnectionSettings $settings = null,
        ClientInterface $client = null,
        RequestFactory $requestFactory = null
    ): SyncUsersManager {
        return new UsersManager(
            $settings ?? ConnectionSettings::default(),
            $client ?? HttpClientDiscovery::find(),
            $requestFactory ?? MessageFactoryDiscovery::find()
        );
    }
}
