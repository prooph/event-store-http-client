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
use Prooph\EventStore\Exception\WrongExpectedVersion;
use Prooph\EventStore\ExpectedVersion;
use ProophTest\EventStoreHttpClient\Helper\EventsStream;
use ProophTest\EventStoreHttpClient\Helper\StreamWriter;
use ProophTest\EventStoreHttpClient\Helper\TailWriter;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;

class appending_to_implicitly_created_stream extends TestCase
{
    /** @test */
    public function sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(6);

        $writer = new StreamWriter($connection, $stream, ExpectedVersion::NO_STREAM);

        $tailWriter = $writer->append($events);
        \assert($tailWriter instanceof TailWriter);
        $tailWriter->then($events[0], ExpectedVersion::NO_STREAM);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total, $events);
    }

    /** @test */
    public function sequence_0em1_1e0_2e1_3e2_4e3_4e4_0any_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_4e4_0any_idempotent';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(6);

        $writer = new StreamWriter($connection, $stream, ExpectedVersion::NO_STREAM);

        $tailWriter = $writer->append($events);
        \assert($tailWriter instanceof TailWriter);
        $tailWriter->then($events[0], ExpectedVersion::ANY);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total, $events);
    }

    /** @test */
    public function sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(6);

        $writer = new StreamWriter($connection, $stream, ExpectedVersion::NO_STREAM);

        $first6 = $writer->append($events);
        \assert($first6 instanceof TailWriter);

        $this->expectException(WrongExpectedVersion::class);
        $first6->then($events[0], 6);
    }

    /** @test */
    public function sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_wev';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(6);

        $writer = new StreamWriter($connection, $stream, ExpectedVersion::NO_STREAM);

        $first6 = $writer->append($events);
        \assert($first6 instanceof TailWriter);

        $this->expectException(WrongExpectedVersion::class);
        $first6->then($events[0], 4);
    }

    /** @test */
    public function sequence_0em1_0e0_non_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_0em1_0e0_non_idempotent';

        $connection = TestConnection::create();

        $events = [TestEvent::newTestEvent()];

        $writer = new StreamWriter($connection, $stream, ExpectedVersion::NO_STREAM);

        $tailWriter = $writer->append($events);
        \assert($tailWriter instanceof TailWriter);

        $tailWriter->then($events[0], 0);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total - 1, $events);
    }

    /** @test */
    public function sequence_0em1_0any_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_0em1_0any_idempotent';

        $connection = TestConnection::create();

        $events = [TestEvent::newTestEvent()];

        $writer = new StreamWriter($connection, $stream, ExpectedVersion::NO_STREAM);

        $tailWriter = $writer->append($events);
        \assert($tailWriter instanceof TailWriter);

        $tailWriter->then($events[0], ExpectedVersion::ANY);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total, $events);
    }

    /** @test */
    public function sequence_0em1_0em1_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_0em1_0em1_idempotent';

        $connection = TestConnection::create();

        $events = [TestEvent::newTestEvent()];

        $writer = new StreamWriter($connection, $stream, ExpectedVersion::NO_STREAM);

        $tailWriter = $writer->append($events);
        \assert($tailWriter instanceof TailWriter);

        $tailWriter->then($events[0], ExpectedVersion::NO_STREAM);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total, $events);
    }

    /** @test */
    public function sequence_0em1_1e0_2e1_1any_1any_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_0em1_1e0_2e1_1any_1any_idempotent';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(3);

        $writer = new StreamWriter($connection, $stream, ExpectedVersion::NO_STREAM);

        $tailWriter = $writer->append($events);
        \assert($tailWriter instanceof TailWriter);

        $tailWriter = $tailWriter->then($events[1], ExpectedVersion::ANY);

        $tailWriter->then($events[1], ExpectedVersion::ANY);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total, $events);
    }

    /** @test */
    public function sequence_S_0em1_1em1_E_S_0em1_E_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_0em1_E_idempotent';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(2);

        $connection->appendToStream($stream, ExpectedVersion::NO_STREAM, $events);
        $connection->appendToStream($stream, ExpectedVersion::NO_STREAM, [$events[0]]);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total, $events);
    }

    /** @test */
    public function sequence_S_0em1_1em1_E_S_0any_E_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_0any_E_idempotent';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(2);

        $connection->appendToStream($stream, ExpectedVersion::NO_STREAM, $events);
        $connection->appendToStream($stream, ExpectedVersion::ANY, [$events[0]]);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total, $events);
    }

    /** @test */
    public function sequence_S_0em1_1em1_E_S_1e0_E_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_1e0_E_idempotent';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(2);

        $connection->appendToStream($stream, ExpectedVersion::NO_STREAM, $events);
        $connection->appendToStream($stream, 0, [$events[1]]);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total, $events);
    }

    /** @test */
    public function sequence_S_0em1_1em1_E_S_1any_E_idempotent(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_1any_E_idempotent';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(2);

        $connection->appendToStream($stream, ExpectedVersion::NO_STREAM, $events);
        $connection->appendToStream($stream, ExpectedVersion::ANY, [$events[1]]);

        $total = EventsStream::count($connection, $stream);

        $this->assertCount($total, $events);
    }

    /** @test */
    public function sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail(): void
    {
        $stream = 'appending_to_implicitly_created_stream_sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotancy_fail';

        $connection = TestConnection::create();

        $events = TestEvent::newAmount(2);

        $connection->appendToStream($stream, ExpectedVersion::NO_STREAM, $events);

        $events[] = TestEvent::newTestEvent();

        $this->expectException(WrongExpectedVersion::class);

        $connection->appendToStream($stream, ExpectedVersion::NO_STREAM, $events);
    }
}
