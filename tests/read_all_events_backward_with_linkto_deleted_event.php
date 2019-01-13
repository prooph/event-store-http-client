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
use Prooph\EventStore\StreamEventsSlice;
use Throwable;

class read_all_events_backward_with_linkto_deleted_event extends TestCase
{
    use SpecificationWithLinkToToDeletedEvents;

    /** @var StreamEventsSlice */
    private $read;

    protected function when(): void
    {
        $this->read = $this->conn->readStreamEventsBackward(
            $this->linkedStreamName,
            0,
            1
        );
    }

    /**
     * @test
     * @throws Throwable
     */
    public function one_event_is_read(): void
    {
        $this->markTestIncomplete('not yet implemented');

        $this->execute(function () {
            $this->assertCount(1, $this->read->events());

            new Success();
        });
    }

    /**
     * @test
     * @throws Throwable
     */
    public function the_linked_event_is_not_resolved(): void
    {
        $this->markTestIncomplete('not yet implemented');

        $this->execute(function () {
            $this->assertNull($this->read->events()[0]->event());

            new Success();
        });
    }

    /**
     * @test
     * @throws Throwable
     */
    public function the_link_event_is_included(): void
    {
        $this->markTestIncomplete('not yet implemented');

        $this->execute(function () {
            $this->assertNotNull($this->read->events()[0]->originalEvent());

            new Success();
        });
    }

    /**
     * @test
     * @throws Throwable
     */
    public function the_event_is_not_resolved(): void
    {
        $this->markTestIncomplete('not yet implemented');

        $this->execute(function () {
            $this->assertFalse($this->read->events()[0]->isResolved());

            new Success();
        });
    }
}
