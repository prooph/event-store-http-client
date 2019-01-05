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
use Prooph\EventStore\StreamEventsSlice;
use Throwable;

class read_all_events_forward_with_linkto_passed_max_count extends TestCase
{
    use SpecificationWithLinkToToMaxCountDeletedEvents;

    /** @var StreamEventsSlice */
    private $read;

    protected function when(): void
    {
        $this->read = $this->conn->readStreamEventsForward($this->linkedStreamName, 0, 1, true);
    }

    /**
     * @test
     * @throws Throwable
     */
    public function one_event_is_read(): void
    {
        $this->execute(function () {
            $this->assertCount(1, $this->read->events());
        });
    }
}
