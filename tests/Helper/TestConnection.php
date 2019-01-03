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

namespace ProophTest\EventStoreHttpClient\Helper;

use Prooph\EventStore\EndPoint;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Prooph\EventStoreHttpClient\EventStoreConnectionFactory;

/** @internal */
class TestConnection
{
    public static function create(?UserCredentials $userCredentials = null): EventStoreConnection
    {
        self::checkRequiredEnvironmentSettings();

        return EventStoreConnectionFactory::create(
            null,
            null,
            self::settings($userCredentials)
        );
    }

    private static function checkRequiredEnvironmentSettings(): void
    {
        $env = \getenv();

        if (! isset(
            $env['ES_HOST'],
            $env['ES_PORT'],
            $env['ES_USER'],
            $env['ES_PASS']
        )) {
            throw new RuntimeException('Environment settings for event store connection not found');
        }
    }

    public static function settings(?UserCredentials $userCredentials = null): ConnectionSettings
    {
        return new ConnectionSettings(
            self::httpEndPoint(),
            'http',
            $userCredentials
        );
    }

    public static function httpEndPoint(): EndPoint
    {
        $env = \getenv();

        if (! isset(
            $env['ES_HOST'],
            $env['ES_HTTP_PORT']
        )) {
            throw new RuntimeException('Environment settings for event store http endpoint not found');
        }

        $host = (string) \getenv('ES_HOST');
        $port = (int) \getenv('ES_HTTP_PORT');

        return new EndPoint($host, $port);
    }
}
