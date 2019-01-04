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

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Exception\StreamDeleted;
use Prooph\EventStore\Exception\WrongExpectedVersion;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\ReadDirection;
use Prooph\EventStore\StreamEventsSlice;
use Prooph\EventStore\StreamMetadata;
use Prooph\EventStore\WriteResult;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;
use Throwable;

class append_to_stream extends TestCase
{
    /** @test */
    public function cannot_append_to_stream_without_name(): void
    {
        $connection = TestConnection::create();
        $this->expectException(InvalidArgumentException::class);
        $connection->appendToStream('', ExpectedVersion::ANY, []);
    }

    /** @test */
    public function should_allow_appending_zero_events_to_stream_with_no_problems(): void
    {
        $stream1 = 'should_allow_appending_zero_events_to_stream_with_no_problems1';
        $stream2 = 'should_allow_appending_zero_events_to_stream_with_no_problems2';

        $connection = TestConnection::create();

        $result = $connection->appendToStream($stream1, ExpectedVersion::ANY, []);
        \assert($result instanceof WriteResult);

        $connection->appendToStream($stream1, ExpectedVersion::NO_STREAM, []);
        $connection->appendToStream($stream1, ExpectedVersion::ANY, []);
        $connection->appendToStream($stream1, ExpectedVersion::NO_STREAM, []);

        $slice = $connection->readStreamEventsForward($stream1, 0, 2, false);
        \assert($slice instanceof StreamEventsSlice);
        $this->assertCount(0, $slice->events());
        $this->assertEquals($stream1, $slice->stream());
        $this->assertEquals(0, $slice->fromEventNumber());
        $this->assertEquals($slice->readDirection()->name(), ReadDirection::forward()->name());

        $connection->appendToStream($stream2, ExpectedVersion::NO_STREAM, []);
        $connection->appendToStream($stream2, ExpectedVersion::ANY, []);
        $connection->appendToStream($stream2, ExpectedVersion::NO_STREAM, []);
        $connection->appendToStream($stream2, ExpectedVersion::ANY, []);

        $slice = $connection->readStreamEventsForward($stream2, 0, 2, false);
        $this->assertCount(0, $slice->events());
    }

    /** @test */
    public function should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist(): void
    {
        $stream = 'should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist';

        $connection = TestConnection::create();

        $result = $connection->appendToStream($stream, ExpectedVersion::NO_STREAM, [TestEvent::newTestEvent()]);
        \assert($result instanceof WriteResult);

        $slice = $connection->readStreamEventsForward($stream, 0, 2, false);
        \assert($slice instanceof StreamEventsSlice);

        $this->assertCount(1, $slice->events());
    }

    /** @test */
    public function should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist(): void
    {
        $stream = 'should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist';

        $connection = TestConnection::create();

        $result = $connection->appendToStream($stream, ExpectedVersion::ANY, [TestEvent::newTestEvent()]);
        \assert($result instanceof WriteResult);

        $slice = $connection->readStreamEventsForward($stream, 0, 2, false);
        \assert($slice instanceof StreamEventsSlice);
        $this->assertCount(1, $slice->events());
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function multiple_idempotent_writes(): void
    {
        $stream = 'multiple_idempotent_writes';

        $connection = TestConnection::create();

        $events = [TestEvent::newTestEvent(), TestEvent::newTestEvent(), TestEvent::newTestEvent(), TestEvent::newTestEvent()];

        $result = $connection->appendToStream($stream, ExpectedVersion::ANY, $events);
        \assert($result instanceof WriteResult);

        $connection->appendToStream($stream, ExpectedVersion::ANY, $events);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function multiple_idempotent_writes_with_same_id_bug_case(): void
    {
        $stream = 'multiple_idempotent_writes_with_same_id_bug_case';

        $connection = TestConnection::create();

        $x = TestEvent::newTestEvent();
        $events = [$x, $x, $x, $x, $x, $x];

        $result = $connection->appendToStream($stream, ExpectedVersion::ANY, $events);
        \assert($result instanceof WriteResult);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function in_wtf_multiple_case_of_multiple_writes_expected_version_any_per_all_same_id(): void
    {
        $stream = 'in_wtf_multiple_case_of_multiple_writes_expected_version_any_per_all_same_id';

        $connection = TestConnection::create();

        $x = TestEvent::newTestEvent();
        $events = [$x, $x, $x, $x, $x, $x];

        $result = $connection->appendToStream($stream, ExpectedVersion::ANY, $events);
        \assert($result instanceof WriteResult);

        $f = $connection->appendToStream($stream, ExpectedVersion::ANY, $events);
        \assert($f instanceof WriteResult);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function in_slightly_reasonable_multiple_case_of_multiple_writes_with_expected_version_per_all_same_id(): void
    {
        $stream = 'in_slightly_reasonable_multiple_case_of_multiple_writes_with_expected_version_per_all_same_id';

        $connection = TestConnection::create();

        $x = TestEvent::newTestEvent();
        $events = [$x, $x, $x, $x, $x, $x];

        $result = $connection->appendToStream($stream, ExpectedVersion::EMPTY_STREAM, $events);
        \assert($result instanceof WriteResult);

        $f = $connection->appendToStream($stream, ExpectedVersion::EMPTY_STREAM, $events);
        \assert($f instanceof WriteResult);
    }

    /** @test */
    public function should_fail_writing_with_correct_exp_ver_to_deleted_stream(): void
    {
        $stream = 'should_fail_writing_with_correct_exp_ver_to_deleted_stream';

        $connection = TestConnection::create();

        $connection->deleteStream($stream, ExpectedVersion::EMPTY_STREAM, true);

        $this->expectException(StreamDeleted::class);
        $connection->appendToStream($stream, ExpectedVersion::NO_STREAM, [TestEvent::newTestEvent()]);
    }

    /** @test */
    public function should_return_log_position_when_writing(): void
    {
        $stream = 'should_return_log_position_when_writing';

        $connection = TestConnection::create();

        $result = $connection->appendToStream($stream, ExpectedVersion::EMPTY_STREAM, [TestEvent::newTestEvent()]);
        \assert($result instanceof WriteResult);
        $this->assertSame(-1, $result->logPosition()->preparePosition());
        $this->assertSame(-1, $result->logPosition()->commitPosition());
    }

    /** @test */
    public function should_fail_writing_with_any_exp_ver_to_deleted_stream(): void
    {
        $stream = 'should_fail_writing_with_any_exp_ver_to_deleted_stream';

        $connection = TestConnection::create();

        try {
            $connection->deleteStream($stream, ExpectedVersion::EMPTY_STREAM, true);
        } catch (Throwable $e) {
            $this->fail($e->getMessage());
        }

        $this->expectException(StreamDeleted::class);
        $connection->appendToStream($stream, ExpectedVersion::ANY, [TestEvent::newTestEvent()]);
    }

    /** @test */
    public function should_fail_writing_with_invalid_exp_ver_to_deleted_stream(): void
    {
        $stream = 'should_fail_writing_with_invalid_exp_ver_to_deleted_stream';

        $connection = TestConnection::create();

        $connection->deleteStream($stream, ExpectedVersion::EMPTY_STREAM, true);

        $this->expectException(StreamDeleted::class);
        $connection->appendToStream($stream, 5, [TestEvent::newTestEvent()]);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function should_append_with_correct_exp_ver_to_existing_stream(): void
    {
        $stream = 'should_append_with_correct_exp_ver_to_existing_stream';

        $connection = TestConnection::create();

        $connection->appendToStream($stream, ExpectedVersion::EMPTY_STREAM, [TestEvent::newTestEvent()]);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function should_append_with_any_exp_ver_to_existing_stream(): void
    {
        $stream = 'should_append_with_any_exp_ver_to_existing_stream';

        $connection = TestConnection::create();

        $result = $connection->appendToStream($stream, ExpectedVersion::EMPTY_STREAM, [TestEvent::newTestEvent()]);
        \assert($result instanceof WriteResult);

        $connection->appendToStream($stream, ExpectedVersion::ANY, [TestEvent::newTestEvent()]);
    }

    /** @test */
    public function should_fail_appending_with_wrong_exp_ver_to_existing_stream(): void
    {
        $stream = 'should_fail_appending_with_wrong_exp_ver_to_existing_stream';

        $connection = TestConnection::create();

        $this->expectException(WrongExpectedVersion::class);
        $connection->appendToStream($stream, 1, [TestEvent::newTestEvent()]);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function should_append_with_stream_exists_exp_ver_to_existing_stream(): void
    {
        $stream = 'should_append_with_stream_exists_exp_ver_to_existing_stream';

        $connection = TestConnection::create();

        $connection->appendToStream($stream, ExpectedVersion::EMPTY_STREAM, [TestEvent::newTestEvent()]);
        $connection->appendToStream($stream, ExpectedVersion::STREAM_EXISTS, [TestEvent::newTestEvent()]);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function should_append_with_stream_exists_exp_ver_to_stream_with_multiple_events(): void
    {
        $stream = 'should_append_with_stream_exists_exp_ver_to_stream_with_multiple_events';

        $connection = TestConnection::create();

        for ($i = 0; $i < 5; $i++) {
            $connection->appendToStream($stream, ExpectedVersion::ANY, [TestEvent::newTestEvent()]);
        }

        $connection->appendToStream($stream, ExpectedVersion::STREAM_EXISTS, [TestEvent::newTestEvent()]);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function should_append_with_stream_exists_exp_ver_if_metadata_stream_exists(): void
    {
        $stream = 'should_append_with_stream_exists_exp_ver_if_metadata_stream_exists';

        $connection = TestConnection::create();

        $connection->setStreamMetadata(
            $stream,
            ExpectedVersion::ANY,
            new StreamMetadata(
                10
            )
        );

        $connection->appendToStream($stream, ExpectedVersion::STREAM_EXISTS, [TestEvent::newTestEvent()]);
    }

    /** @test */
    public function should_fail_appending_with_stream_exists_exp_ver_and_stream_does_not_exist(): void
    {
        $stream = 'should_fail_appending_with_stream_exists_exp_ver_and_stream_does_not_exist';

        $connection = TestConnection::create();

        $this->expectException(WrongExpectedVersion::class);
        $connection->appendToStream($stream, ExpectedVersion::STREAM_EXISTS, [TestEvent::newTestEvent()]);
    }

    /** @test */
    public function should_fail_appending_with_stream_exists_exp_ver_to_hard_deleted_stream(): void
    {
        $stream = 'should_fail_appending_with_stream_exists_exp_ver_and_stream_does_not_exist';

        $connection = TestConnection::create();

        $connection->deleteStream($stream, ExpectedVersion::EMPTY_STREAM, true);

        $this->expectException(StreamDeleted::class);
        $connection->appendToStream($stream, ExpectedVersion::STREAM_EXISTS, [TestEvent::newTestEvent()]);
    }

    /** @test */
    public function should_fail_appending_with_stream_exists_exp_ver_to_soft_deleted_stream(): void
    {
        $stream = 'should_fail_appending_with_stream_exists_exp_ver_to_soft_deleted_stream';

        $connection = TestConnection::create();

        $connection->deleteStream($stream, ExpectedVersion::EMPTY_STREAM, false);

        $this->expectException(StreamDeleted::class);
        $connection->appendToStream($stream, ExpectedVersion::STREAM_EXISTS, [TestEvent::newTestEvent()]);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function can_append_multiple_events_at_once(): void
    {
        $stream = 'can_append_multiple_events_at_once';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(100);

        $result = $connection->appendToStream($stream, ExpectedVersion::EMPTY_STREAM, $events);
        \assert($result instanceof WriteResult);
    }

    /** @test */
    public function writes_predefined_event_id(): void
    {
        $stream = 'writes_predefined_event_id';

        $connection = TestConnection::create();

        $event = TestEvent::newTestEvent();

        $connection->appendToStream($stream, ExpectedVersion::ANY, [$event]);

        $events = $connection->readStreamEventsBackward($stream, -1, 1);
        \assert($events instanceof StreamEventsSlice);

        $readEvent = $events->events()[0]->event();

        $this->assertEquals($event->eventId()->toString(), $readEvent->eventId()->toString());
    }
}
