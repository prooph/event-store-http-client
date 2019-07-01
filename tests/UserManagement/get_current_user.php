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

use Prooph\EventStore\UserManagement\UserDetails;
use ProophTest\EventStoreHttpClient\DefaultData;

class get_current_user extends TestWithNode
{
    /** @test */
    public function returns_the_current_user(): void
    {
        $user = $this->manager->getCurrentUser(DefaultData::adminCredentials());
        \assert($user instanceof UserDetails);

        $this->assertSame('admin', $user->loginName());
        $this->assertSame('Event Store Administrator', $user->fullName());
    }
}
