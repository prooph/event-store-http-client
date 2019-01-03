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

use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\Exception\UserCommandFailed;
use ProophTest\EventStoreHttpClient\DefaultData;

class enable_disable_user extends TestWithUser
{
    /** @test */
    public function disable_empty_username_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->manager->disable('', DefaultData::adminCredentials());
    }

    /** @test */
    public function enable_empty_username_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->manager->enable('', DefaultData::adminCredentials());
    }

    /** @test */
    public function can_enable_disable_user(): void
    {
        $this->manager->disable($this->username, DefaultData::adminCredentials());

        $thrown = false;

        try {
            $this->manager->disable('foo', DefaultData::adminCredentials());
        } catch (UserCommandFailed $e) {
            $thrown = true;
        }

        $this->assertTrue($thrown, UserCommandFailed::class . ' was expected');

        $this->manager->enable($this->username, DefaultData::adminCredentials());

        $this->manager->getCurrentUser(new UserCredentials($this->username, 'password'));
    }
}
