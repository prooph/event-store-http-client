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

namespace ProophTest\EventStoreHttpClient\UserManagement;

use Prooph\EventStore\UserManagement\UserDetails;
use Prooph\EventStoreHttpClient\UserManagement\UsersManagerFactory;
use ProophTest\EventStoreHttpClient\DefaultData;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;

class list_users extends TestWithNode
{
    /** @test */
    public function list_all_users_works(): void
    {
        $this->manager->createUser('ouro', 'ourofull', ['foo', 'bar'], 'ouro', DefaultData::adminCredentials());

        $users = $this->manager->listAll(DefaultData::adminCredentials());

        $this->assertGreaterThanOrEqual(3, \count($users));

        $foundAdmin = false;
        $foundOps = false;
        $foundOuro = false;

        foreach ($users as $user) {
            if ($user->loginName() === 'admin') {
                $foundAdmin = true;
            }

            if ($user->loginName() === 'ops') {
                $foundOps = true;
            }

            if ($user->loginName() === 'ouro') {
                $foundOuro = true;
            }
        }

        $this->assertTrue($foundAdmin);
        $this->assertTrue($foundOps);
        $this->assertTrue($foundOuro);
    }

    /** @test */
    public function list_all_users_falls_back_to_default_credentials(): void
    {
        $manager = UsersManagerFactory::create(
            TestConnection::settings(DefaultData::adminCredentials())
        );

        $manager->createUser('ouro2', 'ourofull', ['foo', 'bar'], 'ouro', DefaultData::adminCredentials());

        /** @var UserDetails[] $users */
        $users = $manager->listAll();

        $this->assertGreaterThanOrEqual(3, $users);

        $foundAdmin = false;
        $foundOps = false;
        $foundOuro = false;

        foreach ($users as $user) {
            if ($user->loginName() === 'admin') {
                $foundAdmin = true;
            }

            if ($user->loginName() === 'ops') {
                $foundOps = true;
            }

            if ($user->loginName() === 'ouro2') {
                $foundOuro = true;
            }
        }

        $this->assertTrue($foundAdmin);
        $this->assertTrue($foundOps);
        $this->assertTrue($foundOuro);
    }
}
