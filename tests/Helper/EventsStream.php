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

/** @internal */
class EventsStream
{
    private const SLICE_SIZE = 10;

    public static function count(EventStoreConnection $connection, string $stream): int
    {
        $result = 0;

        while (true) {
            $slice = $connection->readStreamEventsForward($stream, $result, self::SLICE_SIZE, false);
            $result += \count($slice->events());

            if ($slice->isEndOfStream()) {
                break;
            }
        }

        return $result;
    }
}
