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

namespace ProophTest\EventStoreHttpClient;

use Amp\Success;
use PHPUnit\Framework\TestCase;
use Prooph\EventStore\EventData;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\SliceReadStatus;
use Prooph\EventStore\StreamMetadata;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;

class when_having_max_count_set_for_stream extends TestCase
{
    /** @var string */
    private $stream = 'max-count-test-stream';
    /** @var EventStoreConnection */
    private $conn;
    /** @var EventData[] */
    private $testEvents = [];

    private function execute(callable $function): void
    {
        $this->conn = TestConnection::create();

        $this->conn->setStreamMetadata(
            $this->stream,
            ExpectedVersion::ANY,
            StreamMetadata::create()->setMaxCount(3)->build(),
            DefaultData::adminCredentials()
        );

        for ($i = 0; $i < 5; $i++) {
            $this->testEvents[] = TestEvent::newTestEvent(null, (string) $i);
        }

        $this->conn->appendToStream(
            $this->stream,
            ExpectedVersion::ANY,
            $this->testEvents,
            DefaultData::adminCredentials()
        );

        $function();
    }

    /** @test */
    public function read_stream_forward_respects_max_count(): void
    {
        $this->execute(function (): void {
            $res = $this->conn->readStreamEventsForward(
                $this->stream,
                0,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(3, $res->events());

            for ($i = 0; $i < 3; $i++) {
                $testEvent = $this->testEvents[$i + 2];
                $event = $res->events()[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }
        });
    }

    /** @test */
    public function read_stream_backward_respects_max_count(): void
    {
        $this->execute(function (): void {
            $res = $this->conn->readStreamEventsBackward(
                $this->stream,
                -1,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(3, $res->events());

            for ($i = 0; $i < 3; $i++) {
                $testEvent = $this->testEvents[$i + 2];
                $event = \array_reverse($res->events())[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }
        });
    }

    /** @test */
    public function after_setting_less_strict_max_count_read_stream_forward_reads_more_events(): void
    {
        $this->execute(function (): void {
            $res = $this->conn->readStreamEventsForward(
                $this->stream,
                0,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(3, $res->events());

            for ($i = 0; $i < 3; $i++) {
                $testEvent = $this->testEvents[$i + 2];
                $event = $res->events()[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }

            $this->conn->setStreamMetadata(
                $this->stream,
                ExpectedVersion::ANY,
                StreamMetadata::create()->setMaxCount(4)->build(),
                DefaultData::adminCredentials()
            );

            $res = $this->conn->readStreamEventsForward(
                $this->stream,
                0,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(4, $res->events());

            for ($i = 0; $i < 4; $i++) {
                $testEvent = $this->testEvents[$i + 1];
                $event = $res->events()[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }
        });
    }

    /** @test */
    public function after_setting_more_strict_max_count_read_stream_forward_reads_less_events(): void
    {
        $this->execute(function (): void {
            $res = $this->conn->readStreamEventsForward(
                $this->stream,
                0,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(3, $res->events());

            for ($i = 0; $i < 3; $i++) {
                $testEvent = $this->testEvents[$i + 2];
                $event = $res->events()[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }

            $this->conn->setStreamMetadata(
                $this->stream,
                ExpectedVersion::ANY,
                StreamMetadata::create()->setMaxCount(2)->build(),
                DefaultData::adminCredentials()
            );

            $res = $this->conn->readStreamEventsForward(
                $this->stream,
                0,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(2, $res->events());

            for ($i = 0; $i < 2; $i++) {
                $testEvent = $this->testEvents[$i + 3];
                $event = $res->events()[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }
        });
    }

    /** @test */
    public function after_setting_less_strict_max_count_read_stream_backward_reads_more_events(): void
    {
        $this->execute(function (): void {
            $res = $this->conn->readStreamEventsBackward(
                $this->stream,
                -1,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(3, $res->events());

            for ($i = 0; $i < 3; $i++) {
                $testEvent = $this->testEvents[$i + 2];
                $event = \array_reverse($res->events())[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }

            $this->conn->setStreamMetadata(
                $this->stream,
                ExpectedVersion::ANY,
                StreamMetadata::create()->setMaxCount(4)->build(),
                DefaultData::adminCredentials()
            );

            $res = $this->conn->readStreamEventsBackward(
                $this->stream,
                -1,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(4, $res->events());

            for ($i = 0; $i < 4; $i++) {
                $testEvent = $this->testEvents[$i + 1];
                $event = \array_reverse($res->events())[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }
        });
    }

    /** @test */
    public function after_setting_more_strict_max_count_read_stream_backward_reads_less_events(): void
    {
        $this->execute(function (): void {
            $res = $this->conn->readStreamEventsBackward(
                $this->stream,
                -1,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(3, $res->events());

            for ($i = 0; $i < 3; $i++) {
                $testEvent = $this->testEvents[$i + 2];
                $event = \array_reverse($res->events())[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }

            $this->conn->setStreamMetadata(
                $this->stream,
                ExpectedVersion::ANY,
                StreamMetadata::create()->setMaxCount(2)->build(),
                DefaultData::adminCredentials()
            );

            $res = $this->conn->readStreamEventsBackward(
                $this->stream,
                -1,
                100,
                false,
                DefaultData::adminCredentials()
            );

            $this->assertTrue($res->status()->equals(SliceReadStatus::success()));
            $this->assertCount(2, $res->events());

            for ($i = 0; $i < 2; $i++) {
                $testEvent = $this->testEvents[$i + 3];
                $event = \array_reverse($res->events())[$i];

                $this->assertTrue($testEvent->eventId()->equals($event->event()->eventId()));
            }
        });
    }
}
