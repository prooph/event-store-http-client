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
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\RecordedEvent;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SliceReadStatus;
use Prooph\EventStore\StreamEventsSlice;
use Prooph\EventStore\StreamPosition;
use ProophTest\EventStoreHttpClient\Helper\EventDataComparer;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;

class read_event_stream_forward_should extends TestCase
{
    /** @test */
    public function throw_if_count_le_zero(): void
    {
        $stream = 'read_event_stream_forward_should_throw_if_count_le_zero';

        $store = TestConnection::create();

        $this->expectException(InvalidArgumentException::class);

        $store->readStreamEventsForward(
            $stream,
            0,
            0,
            false
        );
    }

    /** @test */
    public function throw_if_start_lt_zero(): void
    {
        $stream = 'read_event_stream_forward_should_throw_if_start_lt_zero';

        $store = TestConnection::create();

        $this->expectException(InvalidArgumentException::class);

        $store->readStreamEventsForward(
            $stream,
            -1,
            1,
            false
        );
    }

    /** @test */
    public function notify_using_status_code_if_stream_not_found(): void
    {
        $stream = 'read_event_stream_forward_should_notify_using_status_code_if_stream_not_found';

        $store = TestConnection::create();

        $read = $store->readStreamEventsForward(
            $stream,
            StreamPosition::START,
            1,
            false
        );
        \assert($read instanceof StreamEventsSlice);

        $this->assertTrue(SliceReadStatus::streamNotFound()->equals($read->status()));
    }

    /** @test */
    public function notify_using_status_code_if_stream_was_deleted(): void
    {
        $stream = 'read_event_stream_forward_should_notify_using_status_code_if_stream_was_deleted';

        $store = TestConnection::create();

        $store->deleteStream($stream, ExpectedVersion::EMPTY_STREAM, true);

        $read = $store->readStreamEventsForward(
            $stream,
            StreamPosition::START,
            1,
            false
        );
        \assert($read instanceof StreamEventsSlice);

        $this->assertTrue(SliceReadStatus::streamDeleted()->equals($read->status()));
    }

    /** @test */
    public function return_no_events_when_called_on_empty_stream(): void
    {
        $stream = 'read_event_stream_forward_should_return_single_event_when_called_on_empty_stream';

        $store = TestConnection::create();

        $read = $store->readStreamEventsForward(
            $stream,
            StreamPosition::START,
            1,
            false
        );
        \assert($read instanceof StreamEventsSlice);

        $this->assertCount(0, $read->events());
    }

    /** @test */
    public function return_empty_slice_when_called_on_non_existing_range(): void
    {
        $stream = 'read_event_stream_forward_should_return_empty_slice_when_called_on_non_existing_range';

        $store = TestConnection::create();

        $testEvents = TestEvent::newAmount(10);
        $store->appendToStream(
            $stream,
            ExpectedVersion::EMPTY_STREAM,
            $testEvents
        );

        $read = $store->readStreamEventsForward(
            $stream,
            11,
            5,
            false
        );
        \assert($read instanceof StreamEventsSlice);

        $this->assertCount(0, $read->events());
    }

    /** @test */
    public function return_partial_slice_if_no_enough_events_in_stream(): void
    {
        $stream = 'read_event_stream_forward_should_return_partial_slice_if_no_enough_events_in_stream';

        $store = TestConnection::create();

        $testEvents = TestEvent::newAmount(10);
        $store->appendToStream(
            $stream,
            ExpectedVersion::EMPTY_STREAM,
            $testEvents
        );

        $read = $store->readStreamEventsForward(
            $stream,
            9,
            5,
            false
        );
        \assert($read instanceof StreamEventsSlice);

        $this->assertCount(1, $read->events());
    }

    /** @test */
    public function throw_when_got_int_max_value_as_maxcount(): void
    {
        $store = TestConnection::create();

        $this->expectException(InvalidArgumentException::class);

        $store->readStreamEventsForward(
            'foo',
            StreamPosition::START,
            \PHP_INT_MAX,
            false
        );
    }

    /** @test */
    public function return_events_in_same_order_as_written(): void
    {
        $stream = 'read_event_stream_forward_should_return_events_in_same_order_as_written';

        $store = TestConnection::create();

        $testEvents = TestEvent::newAmount(10);
        $store->appendToStream(
            $stream,
            ExpectedVersion::EMPTY_STREAM,
            $testEvents
        );

        $read = $store->readStreamEventsForward(
            $stream,
            StreamPosition::START,
            10,
            false
        );
        \assert($read instanceof StreamEventsSlice);

        $events = \array_map(
            function (ResolvedEvent $e): RecordedEvent {
                return $e->event();
            },
            $read->events()
        );

        $this->assertTrue(EventDataComparer::allEqual($testEvents, $events));
    }

    /** @test */
    public function be_able_to_read_slice_from_arbitrary_position(): void
    {
        $stream = 'read_event_stream_forward_should_be_able_to_read_slice_from_arbitrary_position';

        $store = TestConnection::create();

        $testEvents = TestEvent::newAmount(10);
        $store->appendToStream(
            $stream,
            ExpectedVersion::EMPTY_STREAM,
            $testEvents
        );

        $read = $store->readStreamEventsForward(
            $stream,
            5,
            2,
            false
        );
        \assert($read instanceof StreamEventsSlice);

        $events = \array_map(
            function (ResolvedEvent $e): RecordedEvent {
                return $e->event();
            },
            $read->events()
        );

        $this->assertTrue(EventDataComparer::allEqual(
            \array_slice($testEvents, 5, 2),
            $events
        ));
    }
}
