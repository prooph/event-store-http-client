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

namespace ProophTest\EventStoreHttpClient\Helper;

use Prooph\EventStore\EventData;
use Prooph\EventStore\EventStoreConnection;

/** @internal */
class TailWriter
{
    /** @var EventStoreConnection */
    private $connection;
    /** @var string */
    private $stream;

    public function __construct(EventStoreConnection $connection, string $stream)
    {
        $this->connection = $connection;
        $this->stream = $stream;
    }

    public function then(EventData $event, int $expectedVersion): TailWriter
    {
        $this->connection->appendToStream($this->stream, $expectedVersion, [$event]);

        return $this;
    }
}
