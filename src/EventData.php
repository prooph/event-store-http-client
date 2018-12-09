<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2018-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStoreHttpClient;

use Prooph\EventStoreHttpClient\Exception\InvalidArgumentException;

/**
 * Represents an event to be written.
 */
class EventData
{
    /** @var EventId */
    private $eventId;
    /** @var string */
    private $eventType;
    /** @var bool */
    private $isJson;
    /** @var string */
    private $data;
    /** @var string */
    private $metaData;

    public function __construct(?EventId $eventId, string $eventType, bool $isJson, string $data = '', string $metaData = '')
    {
        if (null === $eventId) {
            $eventId = EventId::generate();
        }

        if (! $isJson && ! empty($metaData)) {
            throw new InvalidArgumentException('Cannot record metadata for non-JSON events');
        }

        $this->eventId = $eventId;
        $this->eventType = $eventType;
        $this->isJson = $isJson;
        $this->data = $data;
        $this->metaData = $metaData;
    }

    public function eventId(): EventId
    {
        return $this->eventId;
    }

    public function eventType(): string
    {
        return $this->eventType;
    }

    public function isJson(): bool
    {
        return $this->isJson;
    }

    public function data(): string
    {
        return $this->data;
    }

    public function metaData(): string
    {
        return $this->metaData;
    }
}
