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
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\Exception\StreamDeleted;
use Prooph\EventStore\Exception\WrongExpectedVersion;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\Util\Guid;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;

class when_working_with_raw_stream_metadata extends TestCase
{
    /** @var string */
    private $stream;
    /** @var EventStoreConnection */
    private $conn;

    protected function setUp(): void
    {
        $this->stream = Guid::generateAsHex();
        $this->conn = TestConnection::create();
    }

    /** @test */
    public function setting_empty_metadata_works(): void
    {
        $this->conn->setStreamMetadata(
            $this->stream,
            ExpectedVersion::EMPTY_STREAM
        );

        $meta = $this->conn->getRawStreamMetadata($this->stream);

        $this->assertEquals($this->stream, $meta->stream());
        $this->assertFalse($meta->isStreamDeleted());
        $this->assertEquals(0, $meta->metastreamVersion());
        $this->assertEquals('', $meta->streamMetadata());
    }

    /** @test */
    public function setting_metadata_few_times_returns_last_metadata(): void
    {
        $this->conn->setStreamMetadata(
            $this->stream,
            ExpectedVersion::EMPTY_STREAM
        );

        $meta = $this->conn->getRawStreamMetadata($this->stream);

        $this->assertEquals($this->stream, $meta->stream());
        $this->assertFalse($meta->isStreamDeleted());
        $this->assertEquals(0, $meta->metastreamVersion());
        $this->assertEquals('', $meta->streamMetadata());

        $this->conn->setStreamMetadata(
            $this->stream,
            0
        );

        $meta = $this->conn->getRawStreamMetadata($this->stream);

        $this->assertEquals($this->stream, $meta->stream());
        $this->assertFalse($meta->isStreamDeleted());
        $this->assertEquals(1, $meta->metastreamVersion());
        $this->assertEquals('', $meta->streamMetadata());
    }

    /** @test */
    public function trying_to_set_metadata_with_wrong_expected_version_fails(): void
    {
        $this->expectException(WrongExpectedVersion::class);

        $this->conn->setStreamMetadata(
            $this->stream,
            5
        );
    }

    /** @test */
    public function setting_metadata_with_expected_version_any_works(): void
    {
        $this->conn->setStreamMetadata(
            $this->stream,
            ExpectedVersion::ANY
        );

        $meta = $this->conn->getRawStreamMetadata($this->stream);

        $this->assertEquals($this->stream, $meta->stream());
        $this->assertFalse($meta->isStreamDeleted());
        $this->assertEquals(0, $meta->metastreamVersion());
        $this->assertEquals('', $meta->streamMetadata());

        $this->conn->setStreamMetadata(
            $this->stream,
            ExpectedVersion::ANY
        );

        $meta = $this->conn->getRawStreamMetadata($this->stream);

        $this->assertEquals($this->stream, $meta->stream());
        $this->assertFalse($meta->isStreamDeleted());
        $this->assertEquals(1, $meta->metastreamVersion());
        $this->assertEquals('', $meta->streamMetadata());
    }

    /** @test */
    public function setting_metadata_for_not_existing_stream_works(): void
    {
        $this->conn->setStreamMetadata(
            $this->stream,
            ExpectedVersion::EMPTY_STREAM
        );

        $meta = $this->conn->getRawStreamMetadata($this->stream);

        $this->assertEquals($this->stream, $meta->stream());
        $this->assertFalse($meta->isStreamDeleted());
        $this->assertEquals(0, $meta->metastreamVersion());
        $this->assertEquals('', $meta->streamMetadata());
    }

    /** @test */
    public function setting_metadata_for_existing_stream_works(): void
    {
        $this->conn->appendToStream(
            $this->stream,
            ExpectedVersion::NO_STREAM,
            TestEvent::newAmount(2)
        );

        $this->conn->setStreamMetadata(
            $this->stream,
            ExpectedVersion::EMPTY_STREAM
        );

        $meta = $this->conn->getRawStreamMetadata($this->stream);

        $this->assertEquals($this->stream, $meta->stream());
        $this->assertFalse($meta->isStreamDeleted());
        $this->assertEquals(0, $meta->metastreamVersion());
        $this->assertEquals('', $meta->streamMetadata());
    }

    /** @test */
    public function setting_metadata_for_deleted_stream_throws_stream_deleted_exception(): void
    {
        $this->conn->deleteStream(
            $this->stream,
            ExpectedVersion::NO_STREAM,
            true
        );

        $this->expectException(StreamDeleted::class);

        $this->conn->setStreamMetadata(
            $this->stream,
            ExpectedVersion::EMPTY_STREAM
        );
    }

    /** @test */
    public function getting_metadata_for_nonexisting_stream_returns_empty_string(): void
    {
        $meta = $this->conn->getRawStreamMetadata($this->stream);

        $this->assertEquals($this->stream, $meta->stream());
        $this->assertFalse($meta->isStreamDeleted());
        $this->assertEquals(-1, $meta->metastreamVersion());
        $this->assertEquals('', $meta->streamMetadata());
    }

    /** @test */
    public function getting_metadata_for_deleted_stream_returns_empty_string_and_signals_stream_deletion(): void
    {
        $this->conn->setStreamMetadata($this->stream, ExpectedVersion::EMPTY_STREAM);

        $this->conn->deleteStream($this->stream, ExpectedVersion::NO_STREAM, true);

        $meta = $this->conn->getRawStreamMetadata($this->stream);

        $this->assertEquals($this->stream, $meta->stream());
        $this->assertTrue($meta->isStreamDeleted());
        $this->assertEquals(EventNumber::DELETED_STREAM, $meta->metastreamVersion());
        $this->assertEquals('', $meta->streamMetadata());
    }
}
