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

namespace Prooph\EventStoreHttpClient\Internal;

use Prooph\EventStore\EventId;
use Prooph\EventStore\RecordedEvent;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\Util\DateTime;
use Prooph\EventStore\Util\Json;

/** @internal */
class ResolvedEventParser
{
    public static function parse(array $entry): ResolvedEvent
    {
        $link = null;

        if ($entry['isLinkMetaData']) {
            $data = $entry['data'];

            if (\is_array($data)) {
                $data = Json::encode($data);
            }

            $metadata = $entry['metaData'] ?? '';

            if (\is_array($metadata)) {
                $metadata = Json::encode($metadata);
            }

            $link = new RecordedEvent(
                $entry['streamId'],
                $entry['eventNumber'],
                EventId::fromString($entry['eventId']),
                $entry['eventType'],
                $entry['isJson'],
                $data,
                $metadata,
                DateTime::create($entry['updated'])
            );
        }

        $data = $link ? $entry['title'] : $entry['data'];

        if (\is_array($data)) {
            $data = Json::encode($data);
        }

        $metadata = $link
            ? $entry['linkMetaData']
            : $entry['metaData'] ?? '';

        if (\is_array($metadata)) {
            $metadata = Json::encode($metadata);
        }

        $eventId = $entry['eventId'];

        if ($link) {
            foreach ($entry['links'] as $elink) {
                if ('ack' === $elink['relation']) {
                    $eventId = \substr($elink['uri'], -36);
                    break;
                }
            }
        }

        $record = new RecordedEvent(
            $entry['positionStreamId'],
            $entry['positionEventNumber'],
            EventId::fromString($eventId),
            $entry['summary'],
            $entry['isJson'],
            $data,
            $metadata,
            DateTime::create($entry['updated'])
        );

        return new ResolvedEvent($record, $link, null);
    }
}
