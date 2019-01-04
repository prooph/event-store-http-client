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
use Prooph\EventStoreHttpClient\ConnectionString;

class connection_string extends TestCase
{
    /** @test */
    public function can_set_string_value(): void
    {
        $settings = ConnectionString::getConnectionSettings('endpoint=testtest:1234');
        $this->assertEquals('testtest', $settings->endPoint()->host());
    }

    /** @test */
    public function can_set_bool_value_with_string(): void
    {
        $settings = ConnectionString::getConnectionSettings('requiremaster=true');
        $this->assertTrue($settings->requireMaster());
    }

    /** @test */
    public function can_set_with_spaces(): void
    {
        $settings = ConnectionString::getConnectionSettings('Require Master=true');
        $this->assertTrue($settings->requireMaster());
    }

    /** @test */
    public function can_set_int(): void
    {
        $settings = ConnectionString::getConnectionSettings('endpoint=testtest:1234');
        $this->assertSame(1234, $settings->endPoint()->port());
    }

    /** @test */
    public function can_set_multiple_values(): void
    {
        $settings = ConnectionString::getConnectionSettings('endpoint=testtest:1234;requiremaster=false');
        $this->assertSame('testtest', $settings->endPoint()->host());
        $this->assertSame(1234, $settings->endPoint()->port());
        $this->assertFalse($settings->requireMaster());
    }

    /** @test */
    public function can_set_mixed_case(): void
    {
        $settings = ConnectionString::getConnectionSettings('rEquIreMaSTeR=false');
        $this->assertFalse($settings->requireMaster());
    }

    /** @test */
    public function can_set_user_credentials(): void
    {
        $settings = ConnectionString::getConnectionSettings('DefaultUserCredentials=foo:bar');
        $this->assertEquals('foo', $settings->defaultUserCredentials()->username());
        $this->assertEquals('bar', $settings->defaultUserCredentials()->password());
    }
}
