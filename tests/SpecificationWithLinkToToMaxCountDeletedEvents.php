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

use Prooph\EventStore\Common\SystemEventTypes;
use Prooph\EventStore\EventData;
use Prooph\EventStore\EventId;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\StreamMetadata;
use Prooph\EventStore\Util\Guid;

trait SpecificationWithLinkToToMaxCountDeletedEvents
{
    use SpecificationWithConnection;

    /** @var string */
    protected $linkedStreamName;
    /** @var string */
    protected $deletedStreamName;

    protected function given(): void
    {
        $creds = DefaultData::adminCredentials();

        $this->deletedStreamName = Guid::generateAsHex();
        $this->linkedStreamName = Guid::generateAsHex();

        $this->conn->appendToStream(
            $this->deletedStreamName,
            ExpectedVersion::ANY,
            [
                new EventData(EventId::generate(), 'testing1', true, \json_encode(['foo' => 4])),
            ],
            $creds
        );

        $this->conn->setStreamMetadata(
            $this->deletedStreamName,
            ExpectedVersion::ANY,
            new StreamMetadata(2)
        );

        $this->conn->appendToStream(
            $this->deletedStreamName,
            ExpectedVersion::ANY,
            [
                new EventData(EventId::generate(), 'testing2', true, \json_encode(['foo' => 4])),
            ]
        );

        $this->conn->appendToStream(
            $this->deletedStreamName,
            ExpectedVersion::ANY,
            [
                new EventData(EventId::generate(), 'testing3', true, \json_encode(['foo' => 4])),
            ]
        );

        $this->conn->appendToStream(
            $this->linkedStreamName,
            ExpectedVersion::ANY,
            [
                new EventData(EventId::generate(), SystemEventTypes::LINK_TO, false, '0@' . $this->deletedStreamName),
            ],
            $creds
        );
    }
}
