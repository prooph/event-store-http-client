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
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\Util\Guid;

trait SpecificationWithLinkToToDeletedEvents
{
    use SpecificationWithConnection;

    /** @var string */
    protected $linkedStreamName;
    /** @var string */
    protected $deletedStreamName;

    protected function given(): void
    {
        $creds = DefaultData::adminCredentials();
        $this->linkedStreamName = Guid::generateAsHex();
        $this->deletedStreamName = Guid::generateAsHex();

        $this->conn->appendToStream(
            $this->deletedStreamName,
            ExpectedVersion::ANY,
            [new EventData(null, 'testing', true, '{"foo":"bar"}')],
            $creds
        );

        $this->conn->appendToStream(
            $this->linkedStreamName,
            ExpectedVersion::ANY,
            [new EventData(null, SystemEventTypes::LINK_TO, false, '0@' . $this->deletedStreamName)],
            $creds
        );

        $this->conn->deleteStream(
            $this->deletedStreamName,
            ExpectedVersion::ANY
        );
    }
}
