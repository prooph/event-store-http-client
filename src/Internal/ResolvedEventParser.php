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
        $record = null;

        if ($entry['isLinkMetaData']) {
            $data = $entry['data'];

            if (\is_array($data)) {
                $data = Json::encode($data);
            }

            $metadata = $entry['metaData'] ?? '';

            if (\is_array($metadata)) {
                $metadata = Json::encode($metadata);
            }

            $record = new RecordedEvent(
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

        $data = $record ? $entry['title'] : $entry['data'];

        if (\is_array($data)) {
            $data = Json::encode($data);
        }

        $metadata = $record
            ? $entry['linkMetaData']
            : $entry['metaData'] ?? '';

        if (\is_array($metadata)) {
            $metadata = Json::encode($metadata);
        }

        $eventId = $entry['eventId'];

        if ($record) {
            foreach ($entry['links'] as $elink) {
                if ('ack' === $elink['relation']) {
                    $eventId = \substr($elink['uri'], -36);
                    break;
                }
            }
        }

        $link = new RecordedEvent(
            $entry['positionStreamId'],
            $entry['positionEventNumber'],
            EventId::fromString($eventId),
            $entry['summary'],
            $entry['isJson'],
            $data,
            $metadata,
            DateTime::create($entry['updated'])
        );

        if (null === $record && null !== $link) {
            $record = $link;
            $link = null;
        }

        return new ResolvedEvent($record ?? $link, $link, null);
    }
}
