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

use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Transport\Http\HttpStatusCode;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\Exception\UserCommandFailed;
use ProophTest\EventStoreHttpClient\DefaultData;

class change_password extends TestWithUser
{
    /** @test */
    public function empty_username_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->manager->changePassword('', 'oldPassword', 'newPassword', DefaultData::adminCredentials());
    }

    /** @test */
    public function empty_current_password_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->manager->changePassword($this->username, '', 'newPassword', DefaultData::adminCredentials());
    }

    /** @test */
    public function empty_new_password_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->manager->changePassword($this->username, 'oldPassword', '', DefaultData::adminCredentials());
    }

    /** @test */
    public function can_change_password(): void
    {
        $this->manager->changePassword($this->username, 'password', 'fubar', new UserCredentials($this->username, 'password'));

        $this->expectException(UserCommandFailed::class);

        try {
            $this->manager->changePassword($this->username, 'password', 'foobar', new UserCredentials($this->username, 'password'));
        } catch (UserCommandFailed $e) {
            $this->assertSame(HttpStatusCode::UNAUTHORIZED, $e->httpStatusCode());

            throw $e;
        }
    }
}
