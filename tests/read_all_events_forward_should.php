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
use Prooph\EventStore\Common\SystemRoles;
use Prooph\EventStore\EventData;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\Position;
use Prooph\EventStore\ReadDirection;
use Prooph\EventStore\RecordedEvent;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\StreamAcl;
use Prooph\EventStore\StreamMetadata;
use Prooph\EventStore\Util\Guid;
use ProophTest\EventStoreHttpClient\Helper\EventDataComparer;
use ProophTest\EventStoreHttpClient\Helper\TestEvent;
use Throwable;

class read_all_events_forward_should extends TestCase
{
    use SpecificationWithConnection;

    /** @var EventData[] */
    private $testEvents;
    /** @var Position */
    private $from;
    /** @var string */
    private $stream;

    protected function when(): void
    {
        $this->conn->setStreamMetadata(
            '$all',
            ExpectedVersion::ANY,
            new StreamMetadata(
                null,
                null,
                null,
                null,
                new StreamAcl(
                    [SystemRoles::ALL],
                    [],
                    [],
                    [],
                    []
                )
            ),
            DefaultData::adminCredentials()
        );

        $result = $this->conn->readAllEventsBackward(Position::end(), 1, false, DefaultData::adminCredentials());
        \assert($result instanceof AllEventsSlice);

        $this->from = $result->nextPosition();
        $this->testEvents = TestEvent::newAmount(20);
        $this->stream = 'read_all_events_forward_should-' . Guid::generateAsHex();

        $this->conn->appendToStream($this->stream, ExpectedVersion::EMPTY_STREAM, $this->testEvents);
    }

    protected function end(): void
    {
        $this->conn->setStreamMetadata(
            '$all',
            ExpectedVersion::ANY,
            new StreamMetadata(),
            DefaultData::adminCredentials()
        );
    }

    /**
     * @test
     * @throws Throwable
     */
    public function return_empty_slice_if_asked_to_read_from_end(): void
    {
        $this->execute(function () {
            $read = $this->conn->readAllEventsForward(Position::end(), 1, false, DefaultData::adminCredentials());
            \assert($read instanceof AllEventsSlice);

            $this->assertTrue($read->isEndOfStream());
            $this->assertCount(0, $read->events());
        });
    }

    /**
     * @test
     * @throws Throwable
     */
    public function return_events_in_same_order_as_written(): void
    {
        $this->execute(function () {
            $read = $this->conn->readAllEventsForward($this->from, \count($this->testEvents) + 10, false, DefaultData::adminCredentials());
            \assert($read instanceof AllEventsSlice);

            $events = \array_map(
                function (ResolvedEvent $e): RecordedEvent {
                    return $e->event();
                },
                \array_slice($read->events(), \count($read->events()) - \count($this->testEvents))
            );

            $this->assertTrue(EventDataComparer::allEqual($this->testEvents, $events));
            $this->assertTrue($read->readDirection()->equals(ReadDirection::forward()));
        });
    }

    /**
     * @test
     * @throws Throwable
     */
    public function be_able_to_read_all_one_by_one_until_end_of_stream(): void
    {
        $this->markTestIncomplete('not yet implemented');

        $this->execute(function () {
            $all = [];
            $position = $this->from;
            $slice = null;

            while (true) {
                $slice = $this->conn->readAllEventsForward($position, 1, false, DefaultData::adminCredentials());
                \assert($slice instanceof AllEventsSlice);

                if ($slice->isEndOfStream()) {
                    break;
                }

                $all[] = $slice->events()[0]->event();
                $position = $slice->nextPosition();
            }

            $events = \array_slice($all, \count($all) - \count($this->testEvents));

            $this->assertTrue(EventDataComparer::allEqual($this->testEvents, $events));
            $this->assertTrue($slice->fromPosition()->equals($position));
        });
    }

    /**
     * @test
     * @throws Throwable
     */
    public function be_able_to_read_events_slice_at_time(): void
    {
        $this->markTestIncomplete('not yet implemented');

        $this->execute(function () {
            $all = [];
            $position = $this->from;

            do {
                $slice = $this->conn->readAllEventsForward($position, 5, false, DefaultData::adminCredentials());
                \assert($slice instanceof AllEventsSlice);

                foreach ($slice->events() as $event) {
                    $all[] = $event->event();
                }

                $position = $slice->nextPosition();
            } while (! $slice->isEndOfStream());

            $events = \array_slice($all, \count($all) - \count($this->testEvents));

            $this->assertTrue(EventDataComparer::allEqual($this->testEvents, $events));
        });
    }

    /**
     * @test
     * @throws Throwable
     */
    public function return_partial_slice_if_not_enough_events(): void
    {
        $this->execute(function () {
            $read = $this->conn->readAllEventsForward($this->from, 30, false, DefaultData::adminCredentials());
            \assert($read instanceof AllEventsSlice);

            $this->assertLessThan(30, \count($read->events()));

            $events = \array_map(
                function (ResolvedEvent $e): RecordedEvent {
                    return $e->event();
                },
                \array_slice($read->events(), \count($read->events()) - \count($this->testEvents))
            );

            $this->assertTrue(EventDataComparer::allEqual($this->testEvents, $events));
        });
    }

    /**
     * @test
     * @throws Throwable
     */
    public function throw_when_got_int_max_value_as_maxcount(): void
    {
        $this->execute(function () {
            $this->expectException(InvalidArgumentException::class);

            $this->conn->readAllEventsForward(Position::start(), \PHP_INT_MAX, false);
        });
    }
}
