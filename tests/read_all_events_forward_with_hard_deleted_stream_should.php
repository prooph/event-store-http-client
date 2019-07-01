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

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\AllEventsSlice;
use Prooph\EventStore\Common\SystemEventTypes;
use Prooph\EventStore\EventData;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\Position;
use Prooph\EventStore\SliceReadStatus;
use Prooph\EventStore\StreamEventsSlice;
use ProophTest\EventStoreHttpClient\Helper\EventDataComparer;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;
use Throwable;

class read_all_events_forward_with_hard_deleted_stream_should extends TestCase
{
    use SpecificationWithConnection;

    /** @var EventData[] */
    private $testEvents;
    /** @var string */
    private $streamName;
    /** @var Position */
    private $from;

    protected function when(): void
    {
        $this->streamName = 'read_all_events_forward_with_hard_deleted_stream_should' . $this->getName();

        $result = $this->conn->readAllEventsBackward(Position::end(), 1, false, DefaultData::adminCredentials());
        \assert($result instanceof AllEventsSlice);

        $this->from = $result->nextPosition();

        $this->testEvents = TestEvent::newAmount(20);

        $this->conn->appendToStream(
            $this->streamName,
            ExpectedVersion::EMPTY_STREAM,
            $this->testEvents
        );

        $this->conn->deleteStream(
            $this->streamName,
            ExpectedVersion::ANY,
            true
        );
    }

    /**
     * @test
     * @throws Throwable
     */
    public function ensure_deleted_stream(): void
    {
        $this->execute(function () {
            $res = $this->conn->readStreamEventsForward($this->streamName, 0, 100, false, DefaultData::adminCredentials());
            \assert($res instanceof StreamEventsSlice);
            $this->assertTrue($res->status()->equals(SliceReadStatus::streamDeleted()));
            $this->assertCount(0, $res->events());
        });
    }

    /**
     * @test
     * @throws Throwable
     */
    public function returns_all_events_including_tombstone(): void
    {
        $this->execute(function () {
            $read = $this->conn->readAllEventsForward($this->from, \count($this->testEvents) + 10, false, DefaultData::adminCredentials());
            \assert($read instanceof AllEventsSlice);

            $events = [];
            foreach ($read->events() as $event) {
                $events[] = $event->event();
            }

            $this->assertTrue(
                EventDataComparer::allEqual(
                    $this->testEvents,
                    \array_slice($events, \count($events) - \count($this->testEvents) - 1, \count($this->testEvents))
                )
            );

            $lastEvent = \end($events);

            $this->assertSame($this->streamName, $lastEvent->eventStreamId());
            $this->assertSame(SystemEventTypes::STREAM_DELETED, $lastEvent->eventType());
        });
    }
}
