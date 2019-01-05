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

use Prooph\EventStore\EventData;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStore\Util\Guid;
use Prooph\EventStoreHttpClient\Projections\ProjectionsManager;
use Prooph\EventStoreHttpClient\Projections\ProjectionsManagerFactory;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;

trait ProjectionSpecification
{
    /** @var ProjectionsManager */
    protected $projectionsManager;
    /** @var EventStoreConnection */
    protected $connection;
    /** @var UserCredentials */
    protected $credentials;

    protected function given(): void
    {
    }

    abstract protected function when(): void;

    protected function execute(callable $test): void
    {
        $this->credentials = DefaultData::adminCredentials();
        $this->connection = TestConnection::create();

        $this->projectionsManager = ProjectionsManagerFactory::create(
            null,
            null,
            TestConnection::settings()
        );

        $this->given();
        $this->when();
        $test();
    }

    protected function createEvent(string $eventType, string $data): EventData
    {
        return new EventData(null, $eventType, true, $data);
    }

    protected function postEvent(string $stream, string $eventType, string $data): void
    {
        $this->connection->appendToStream(
            $stream,
            ExpectedVersion::ANY,
            [$this->createEvent($eventType, $data)]
        );
    }

    protected function createOneTimeProjection(): void
    {
        $query = $this->createStandardQuery(Guid::generateAsHex());

        $this->projectionsManager->createOneTime($query, 'JS', $this->credentials);
    }

    protected function createContinuousProjection(string $projectionName): void
    {
        $query = $this->createStandardQuery(Guid::generateAsHex());

        $this->projectionsManager->createContinuous(
            $projectionName,
            $query,
            false,
            'JS',
            $this->credentials
        );
    }

    protected function createStandardQuery(string $stream): string
    {
        return <<<QUERY
fromStream('$stream').when({
    '\$any': function (s, e) {
        s.count = 1;
        return s;
    }
});
QUERY;
    }

    protected function createEmittingQuery(string $stream, string $emittingStream): string
    {
        return <<<QUERY
fromStream('$stream').when({
    '\$any': function (s, e) {
        emit('$emittingStream', 'emittedEvent', e);
    }
});
QUERY;
    }
}
