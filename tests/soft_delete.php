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
use Prooph\EventStore\EventData;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\Exception\StreamDeleted;
use Prooph\EventStore\Exception\WrongExpectedVersion;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\RawStreamMetadataResult;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SliceReadStatus;
use Prooph\EventStore\StreamEventsSlice;
use Prooph\EventStore\StreamMetadata;
use Prooph\EventStore\StreamMetadataResult;
use Prooph\EventStore\WriteResult;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;
use Throwable;

class soft_delete extends TestCase
{
    /** @var EventStoreConnection */
    private $conn;

    protected function setUpTestCase(): void
    {
        $this->conn = TestConnection::create(DefaultData::adminCredentials());
    }

    protected function execute(callable $callback): void
    {
        $this->setUpTestCase();

        $callback();
    }

    /** @test */
    public function soft_deleted_stream_returns_no_stream_and_no_events_on_read(): void
    {
        $this->execute(function () {
            $stream = 'soft_deleted_stream_returns_no_stream_and_no_events_on_read';

            $result = $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->deleteStream($stream, 1);

            \usleep(100000); // wait for the server

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::streamNotFound()->equals($result->status()));
            $this->assertCount(0, $result->events());
        });
    }

    /** @test */
    public function soft_deleted_stream_allows_recreation_when_expver_any(): void
    {
        $this->execute(function () {
            $stream = 'soft_deleted_stream_allows_recreation_when_expver_any';

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->deleteStream($stream, 1);

            \usleep(100000); // wait for the server

            $events = TestEvent::newAmount(3);

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::ANY,
                $events
            );

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::success()->equals($result->status()));
            $this->assertSame(4, $result->lastEventNumber());
            $this->assertCount(3, $result->events());

            $expectedIds = \array_map(
                function (EventData $eventData): string {
                    return $eventData->eventId()->toString();
                },
                $events
            );

            $actualIds = \array_map(
                function (ResolvedEvent $resolvedEvent): string {
                    return $resolvedEvent->originalEvent()->eventId()->toString();
                },
                $result->events()
            );

            $this->assertSame($expectedIds, $actualIds);

            $actualNumbers = \array_map(
                function (ResolvedEvent $resolvedEvent): int {
                    return $resolvedEvent->originalEvent()->eventNumber();
                },
                $result->events()
            );

            $this->assertSame([2, 3, 4], $actualNumbers);

            $meta = $this->conn->getStreamMetadata($stream);

            $this->assertSame(2, $meta->streamMetadata()->truncateBefore());
            $this->assertSame(1, $meta->metastreamVersion());
        });
    }

    /** @test */
    public function soft_deleted_stream_allows_recreation_when_expver_no_stream(): void
    {
        $this->execute(function () {
            $stream = 'soft_deleted_stream_allows_recreation_when_expver_no_stream';

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->deleteStream($stream, 1);

            \usleep(100000); // wait for the server

            $events = TestEvent::newAmount(3);

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                $events
            );

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::success()->equals($result->status()));
            $this->assertSame(4, $result->lastEventNumber());
            $this->assertCount(3, $result->events());

            $expectedIds = \array_map(
                function (EventData $eventData): string {
                    return $eventData->eventId()->toString();
                },
                $events
            );

            $actualIds = \array_map(
                function (ResolvedEvent $resolvedEvent): string {
                    return $resolvedEvent->originalEvent()->eventId()->toString();
                },
                $result->events()
            );

            $this->assertSame($expectedIds, $actualIds);

            $actualNumbers = \array_map(
                function (ResolvedEvent $resolvedEvent): int {
                    return $resolvedEvent->originalEvent()->eventNumber();
                },
                $result->events()
            );

            $this->assertSame([2, 3, 4], $actualNumbers);

            $meta = $this->conn->getStreamMetadata($stream);

            $this->assertSame(2, $meta->streamMetadata()->truncateBefore());
            $this->assertSame(1, $meta->metastreamVersion());
        });
    }

    /** @test */
    public function soft_deleted_stream_allows_recreation_when_expver_is_exact(): void
    {
        $this->execute(function () {
            $stream = 'soft_deleted_stream_allows_recreation_when_expver_is_exact';

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->deleteStream($stream, 1);

            \usleep(100000); // wait for the server

            $events = TestEvent::newAmount(3);

            $this->conn->appendToStream(
                $stream,
                1,
                $events
            );

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::success()->equals($result->status()));
            $this->assertSame(4, $result->lastEventNumber());
            $this->assertCount(3, $result->events());

            $expectedIds = \array_map(
                function (EventData $eventData): string {
                    return $eventData->eventId()->toString();
                },
                $events
            );

            $actualIds = \array_map(
                function (ResolvedEvent $resolvedEvent): string {
                    return $resolvedEvent->originalEvent()->eventId()->toString();
                },
                $result->events()
            );

            $this->assertSame($expectedIds, $actualIds);

            $actualNumbers = \array_map(
                function (ResolvedEvent $resolvedEvent): int {
                    return $resolvedEvent->originalEvent()->eventNumber();
                },
                $result->events()
            );

            $this->assertSame([2, 3, 4], $actualNumbers);

            $meta = $this->conn->getStreamMetadata($stream);

            $this->assertSame(2, $meta->streamMetadata()->truncateBefore());
            $this->assertSame(1, $meta->metastreamVersion());
        });
    }

    /** @test */
    public function soft_deleted_stream_when_recreated_preserves_metadata_except_truncatebefore(): void
    {
        $this->execute(function () {
            $stream = 'soft_deleted_stream_when_recreated_preserves_metadata_except_truncatebefore';

            $result = $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->setStreamMetadata(
                $stream,
                ExpectedVersion::NO_STREAM,
                StreamMetadata::create()
                    ->setTruncateBefore(\PHP_INT_MAX)
                    ->setMaxCount(100)
                    ->setDeleteRoles('some-role')
                    ->setCustomProperty('key1', true)
                    ->setCustomProperty('key2', 17)
                    ->setCustomProperty('key3', 'some value')
                    ->build()
            );

            $events = TestEvent::newAmount(3);

            $this->conn->appendToStream(
                $stream,
                1,
                $events
            );

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::success()->equals($result->status()));
            $this->assertSame(4, $result->lastEventNumber());
            $this->assertCount(3, $result->events());

            $actualNumbers = \array_map(
                function (ResolvedEvent $resolvedEvent): int {
                    return $resolvedEvent->originalEvent()->eventNumber();
                },
                $result->events()
            );

            $this->assertSame([2, 3, 4], $actualNumbers);

            $meta = $this->conn->getStreamMetadata($stream);

            $this->assertSame(1, $meta->metastreamVersion());
            $this->assertSame(2, $meta->streamMetadata()->truncateBefore());
            $this->assertSame(100, $meta->streamMetadata()->maxCount());
            $this->assertSame(['some-role'], $meta->streamMetadata()->acl()->deleteRoles());
            $this->assertTrue($meta->streamMetadata()->getValue('key1'));
            $this->assertSame(17, $meta->streamMetadata()->getValue('key2'));
            $this->assertSame('some value', $meta->streamMetadata()->getValue('key3'));
        });
    }

    /** @test */
    public function soft_deleted_stream_can_be_hard_deleted(): void
    {
        $this->execute(function () {
            $stream = 'soft_deleted_stream_can_be_deleted';

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->deleteStream($stream, 1);
            $this->conn->deleteStream($stream, ExpectedVersion::ANY, true);

            \usleep(100000); // wait for the server

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::streamDeleted()->equals($result->status()));

            $meta = $this->conn->getStreamMetadata($stream);

            $this->assertTrue($meta->isStreamDeleted());

            $this->expectException(StreamDeleted::class);

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::ANY,
                TestEvent::newAmount(1)
            );
        });
    }

    /** @test */
    public function soft_deleted_stream_allows_recreation_only_for_first_write(): void
    {
        $this->execute(function () {
            $stream = 'soft_deleted_stream_allows_recreation_only_for_first_write';

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->deleteStream($stream, 1);

            \usleep(100000); // wait for the server

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(3)
            );

            try {
                $this->conn->appendToStream(
                    $stream,
                    ExpectedVersion::NO_STREAM,
                    TestEvent::newAmount(1)
                );

                $this->fail('Should have thrown');
            } catch (Throwable $e) {
                $this->assertInstanceOf(WrongExpectedVersion::class, $e);
            }

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::success()->equals($result->status()));
            $this->assertSame(4, $result->lastEventNumber());
            $this->assertCount(3, $result->events());

            $actualNumbers = \array_map(
                function (ResolvedEvent $resolvedEvent): int {
                    return $resolvedEvent->originalEvent()->eventNumber();
                },
                $result->events()
            );

            $this->assertSame([2, 3, 4], $actualNumbers);

            $meta = $this->conn->getStreamMetadata($stream);

            $this->assertSame(2, $meta->streamMetadata()->truncateBefore());
            $this->assertSame(1, $meta->metastreamVersion());
        });
    }

    /** @test */
    public function soft_deleted_stream_appends_both_concurrent_writes_when_expver_any(): void
    {
        $this->execute(function () {
            $stream = 'soft_deleted_stream_appends_both_concurrent_writes_when_expver_any';

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->deleteStream($stream, 1);

            \usleep(100000); // wait for the server

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::ANY,
                TestEvent::newAmount(3)
            );

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::ANY,
                TestEvent::newAmount(2)
            );

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::success()->equals($result->status()));
            $this->assertSame(6, $result->lastEventNumber());
            $this->assertCount(5, $result->events());

            $actualNumbers = \array_map(
                function (ResolvedEvent $resolvedEvent): int {
                    return $resolvedEvent->originalEvent()->eventNumber();
                },
                $result->events()
            );

            $this->assertSame([2, 3, 4, 5, 6], $actualNumbers);

            $meta = $this->conn->getStreamMetadata($stream);

            $this->assertSame(2, $meta->streamMetadata()->truncateBefore());
            $this->assertSame(1, $meta->metastreamVersion());
        });
    }

    /** @test */
    public function setting_json_metadata_on_empty_soft_deleted_stream_recreates_stream_not_overriding_metadata(): void
    {
        $this->execute(function () {
            $stream = 'setting_json_metadata_on_empty_soft_deleted_stream_recreates_stream_not_overriding_metadata';

            $this->conn->deleteStream($stream, ExpectedVersion::NO_STREAM, false);

            \usleep(100000); // wait for the server

            $this->conn->setStreamMetadata(
                $stream,
                0,
                StreamMetadata::create()
                    ->setTruncateBefore(\PHP_INT_MAX)
                    ->setMaxCount(100)
                    ->setDeleteRoles('some-role')
                    ->setCustomProperty('key1', true)
                    ->setCustomProperty('key2', 17)
                    ->setCustomProperty('key3', 'some value')
                    ->build()
            );

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::streamNotFound()->equals($result->status()));
            $this->assertCount(0, $result->events());

            $meta = $this->conn->getStreamMetadata($stream);

            $this->assertSame(2, $meta->metastreamVersion());
            $this->assertSame(0, $meta->streamMetadata()->truncateBefore());
            $this->assertSame(100, $meta->streamMetadata()->maxCount());
            $this->assertSame(['some-role'], $meta->streamMetadata()->acl()->deleteRoles());
            $this->assertTrue($meta->streamMetadata()->getValue('key1'));
            $this->assertSame(17, $meta->streamMetadata()->getValue('key2'));
            $this->assertSame('some value', $meta->streamMetadata()->getValue('key3'));
        });
    }

    /** @test */
    public function setting_json_metadata_on_nonempty_soft_deleted_stream_recreates_stream_not_overriding_metadata(): void
    {
        $this->execute(function () {
            $stream = 'setting_json_metadata_on_nonempty_soft_deleted_stream_recreates_stream_not_overriding_metadata';

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->deleteStream($stream, 1, false);

            \usleep(100000); // wait for the server

            $this->conn->setStreamMetadata(
                $stream,
                0,
                StreamMetadata::create()
                    ->setTruncateBefore(\PHP_INT_MAX)
                    ->setMaxCount(100)
                    ->setDeleteRoles('some-role')
                    ->setCustomProperty('key1', true)
                    ->setCustomProperty('key2', 17)
                    ->setCustomProperty('key3', 'some value')
                    ->build()
            );

            $result = $this->conn->readStreamEventsForward(
                $stream,
                0,
                100,
                false
            );

            $this->assertTrue(SliceReadStatus::success()->equals($result->status()));
            $this->assertCount(0, $result->events());

            $meta = $this->conn->getStreamMetadata($stream);

            $this->assertSame(2, $meta->metastreamVersion());
            $this->assertSame(2, $meta->streamMetadata()->truncateBefore());
            $this->assertSame(100, $meta->streamMetadata()->maxCount());
            $this->assertSame(['some-role'], $meta->streamMetadata()->acl()->deleteRoles());
            $this->assertTrue($meta->streamMetadata()->getValue('key1'));
            $this->assertSame(17, $meta->streamMetadata()->getValue('key2'));
            $this->assertSame('some value', $meta->streamMetadata()->getValue('key3'));
        });
    }

    /** @test */
    public function setting_nonjson_metadata_on_empty_soft_deleted_stream_recreates_stream_keeping_original_metadata(): void
    {
        $this->execute(function () {
            $stream = 'setting_nonjson_metadata_on_empty_soft_deleted_stream_recreates_stream_overriding_metadata';
            $metadata = \base64_encode(\random_bytes(256));

            $this->conn->deleteStream($stream, ExpectedVersion::NO_STREAM, false);

            \usleep(100000); // wait for the server

            $this->conn->setRawStreamMetadata($stream, 0, $metadata);

            $result = $this->conn->readStreamEventsForward($stream, 0, 100, false);

            $this->assertTrue(SliceReadStatus::streamNotFound()->equals($result->status()));
            $this->assertCount(0, $result->events());

            $meta = $this->conn->getRawStreamMetadata($stream);

            $this->assertSame(1, $meta->metastreamVersion());
            $this->assertSame($metadata, $meta->streamMetadata());
        });
    }

    /** @test */
    public function setting_nonjson_metadata_on_nonempty_soft_deleted_stream_recreates_stream_keeping_original_metadata(): void
    {
        $this->execute(function () {
            $stream = 'setting_nonjson_metadata_on_nonempty_soft_deleted_stream_recreates_stream_overriding_metadata';
            $metadata = \base64_encode(\random_bytes(256));

            $this->conn->appendToStream(
                $stream,
                ExpectedVersion::NO_STREAM,
                TestEvent::newAmount(2)
            );

            $this->conn->deleteStream($stream, 1, false);

            \usleep(100000); // wait for the server

            $this->conn->setRawStreamMetadata(
                $stream,
                0,
                $metadata
            );

            $result = $this->conn->readStreamEventsForward($stream, 0, 100, false);

            $this->assertTrue(SliceReadStatus::success()->equals($result->status()));
            $this->assertSame(1, $result->lastEventNumber());
            $this->assertCount(2, $result->events());

            $meta = $this->conn->getRawStreamMetadata($stream);

            $this->assertSame($metadata, $meta->streamMetadata());
        });
    }
}
