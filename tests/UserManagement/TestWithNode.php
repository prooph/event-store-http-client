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

namespace ProophTest\EventStoreHttpClient\UserManagement;

use PHPUnit\Framework\TestCase;
use Prooph\EventStoreHttpClient\UserManagement\UsersManager;
use Prooph\EventStoreHttpClient\UserManagement\UsersManagerFactory;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;

abstract class TestWithNode extends TestCase
{
    /** @var UsersManager */
    protected $manager;

    protected function setUp(): void
    {
        $this->manager = UsersManagerFactory::create(
            TestConnection::settings()
        );
    }
}
