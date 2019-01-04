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
use Prooph\EventStore\DeleteResult;
use Prooph\EventStore\Exception\WrongExpectedVersion;
use Prooph\EventStore\ExpectedVersion;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;

class deleting_stream extends TestCase
{
    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function which_doesnt_exists_should_success_when_passed_empty_stream_expected_version(): void
    {
        $stream = 'which_already_exists_should_success_when_passed_empty_stream_expected_version';

        $connection = TestConnection::create();

        $connection->deleteStream($stream, ExpectedVersion::EMPTY_STREAM, true);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function which_doesnt_exists_should_success_when_passed_any_for_expected_version(): void
    {
        $stream = 'which_already_exists_should_success_when_passed_any_for_expected_version';

        $connection = TestConnection::create();

        $connection->deleteStream($stream, ExpectedVersion::ANY, true);
    }

    /** @test */
    public function with_invalid_expected_version_should_fail(): void
    {
        $stream = 'with_invalid_expected_version_should_fail';

        $connection = TestConnection::create();

        $this->expectException(WrongExpectedVersion::class);
        $connection->deleteStream($stream, 1, true);
    }

    /** @test */
    public function should_return_log_position_when_writing(): void
    {
        $this->markTestSkipped('see https://github.com/EventStore/EventStore/issues/1814');

        $stream = 'delete_should_return_log_position_when_writing';

        $connection = TestConnection::create();

        $connection->appendToStream($stream, ExpectedVersion::EMPTY_STREAM, [TestEvent::newTestEvent()]);

        $delete = $connection->deleteStream($stream, 0, true);
        \assert($delete instanceof DeleteResult);

        $this->assertGreaterThan(0, $delete->logPosition()->preparePosition());
        $this->assertGreaterThan(0, $delete->logPosition()->commitPosition());
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function which_was_already_deleted_should_succeed(): void
    {
        // note: this is different from tcp api, there it will fail!
        $stream = 'which_was_allready_deleted_should_fail';

        $connection = TestConnection::create();

        $connection->deleteStream($stream, ExpectedVersion::EMPTY_STREAM, true);
        $connection->deleteStream($stream, ExpectedVersion::EMPTY_STREAM, true);
    }
}
