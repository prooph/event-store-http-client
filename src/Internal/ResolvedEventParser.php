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

use DateTimeImmutable;
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

        if (isset($entry['isLinkMetaData']) && $entry['isLinkMetaData']) {
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
                self::createDateTime($entry['updated'])
            );
        }

        $data = $record ? $entry['title'] : $entry['data'] ?? $entry['title'];

        if (\is_array($data)) {
            $data = Json::encode($data);
        }

        $metadata = $record
            ? $entry['linkMetaData']
            : $entry['metaData'] ?? '';

        if (\is_array($metadata)) {
            $metadata = Json::encode($metadata);
        }

        $eventId = $entry['eventId'] ?? null;

        if ($record) {
            foreach ($entry['links'] as $elink) {
                if ('ack' === $elink['relation']) {
                    $eventId = \substr($elink['uri'], -36);
                    break;
                }
            }
        }

        if (! isset($entry['positionEventNumber'])
            || ! isset($entry['positionStreamId'])
        ) {
            $matches = [];
            \preg_match('/^(\d+)@(.+)$/', $entry['title'], $matches);
            $entry['positionEventNumber'] = (int) $matches[1];
            $entry['positionStreamId'] = $matches[2];
        }

        $link = new RecordedEvent(
            $entry['positionStreamId'],
            $entry['positionEventNumber'],
            EventId::fromString($eventId ?? '00000000-0000-0000-0000-000000000000'),
            $entry['summary'],
            $entry['isJson'] ?? false,
            $data,
            $metadata,
            self::createDateTime($entry['updated'])
        );

        if (null === $record && null !== $link) {
            $record = $link;
            $link = null;
        }

        return new ResolvedEvent($record ?? $link, $link, null);
    }

    private static function createDateTime(string $dateTimeString): DateTimeImmutable
    {
        $micros = \substr($dateTimeString, 20, -1);
        $length = \strlen($micros);

        if ($length < 6) {
            $micros .= \str_repeat('0', 6 - $length);
        } elseif ($length > 6) {
            $micros = \substr($micros, 0, 6);
        }

        $dateTimeString = \substr($dateTimeString, 0, 20) . $micros . 'Z';

        return DateTime::create($dateTimeString);
    }
}
