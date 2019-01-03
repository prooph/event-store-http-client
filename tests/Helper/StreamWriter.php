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

use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\WriteResult;

/** @internal */
class StreamWriter
{
    /** @var EventStoreConnection */
    private $connection;
    /** @var string */
    private $stream;
    /** @var int */
    private $version;

    public function __construct(EventStoreConnection $connection, string $stream, int $version)
    {
        $this->connection = $connection;
        $this->stream = $stream;
        $this->version = $version;
    }

    public function append(array $events): TailWriter
    {
        foreach ($events as $key => $event) {
            $expVer = $this->version === ExpectedVersion::ANY ? ExpectedVersion::ANY : $this->version + $key;
            $result = $this->connection->appendToStream($this->stream, $expVer, [$event]);
            \assert($result instanceof WriteResult);
            $nextExpVer = $result->nextExpectedVersion();

            if ($this->version !== ExpectedVersion::ANY
                && ($expVer + 1) !== $nextExpVer
            ) {
                throw new RuntimeException('Wrong next expected version');
            }
        }

        new TailWriter($this->connection, $this->stream);
    }
}
