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
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\RawStreamMetadataResult;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;

class when_working_with_metadata extends TestCase
{
    /** @test */
    public function when_getting_metadata_for_an_existing_stream_and_no_metadata_exists(): void
    {
        $connection = TestConnection::create();

        $stream = 'when_getting_metadata_for_an_existing_stream_and_no_metadata_exists';

        $connection->appendToStream(
            $stream,
            ExpectedVersion::EMPTY_STREAM,
            [TestEvent::newTestEvent()]
        );

        $meta = $connection->getRawStreamMetadata($stream);
        \assert($meta instanceof RawStreamMetadataResult);

        $this->assertSame($stream, $meta->stream());
        $this->assertFalse($meta->isStreamDeleted());
        $this->assertSame(-1, $meta->metastreamVersion());
        $this->assertEmpty($meta->streamMetadata());
    }
}
