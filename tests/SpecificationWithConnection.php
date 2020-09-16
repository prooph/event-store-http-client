<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2020 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2018-2020 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStoreHttpClient;

use Prooph\EventStore\EventStoreConnection;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;

trait SpecificationWithConnection
{
    /** @var EventStoreConnection */
    protected $conn;

    protected function given(): void
    {
    }

    protected function when(): void
    {
    }

    protected function execute(callable $test): void
    {
        $this->conn = TestConnection::create(DefaultData::adminCredentials());

        $this->given();

        $this->when();

        $test();

        $this->end();
    }

    protected function end(): void
    {
    }
}
