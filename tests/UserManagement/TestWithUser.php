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

namespace ProophTest\EventStoreHttpClient\UserManagement;

use Prooph\EventStore\Util\Guid;
use ProophTest\EventStoreHttpClient\DefaultData;

abstract class TestWithUser extends TestWithNode
{
    /** @var string */
    protected $username;

    protected function setUp(): void
    {
        parent::setUp();

        $this->username = Guid::generateString();

        $this->manager->createUser(
            $this->username,
            'name',
            ['foo', 'admins'],
            'password',
            DefaultData::adminCredentials()
        );
    }
}
