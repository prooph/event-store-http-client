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

namespace ProophTest\EventStoreHttpClient;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\Util\Guid;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;

class create_persistent_subscription_with_dont_timeout extends TestCase
{
    /** @var EventStoreConnection */
    protected $conn;
    /** @var string */
    private $stream;
    /** @var PersistentSubscriptionSettings */
    private $settings;

    protected function setUp(): void
    {
        $this->conn = TestConnection::create();
        $this->stream = Guid::generateAsHex();
        $this->settings = PersistentSubscriptionSettings::create()
            ->doNotResolveLinkTos()
            ->startFromCurrent()
            ->dontTimeoutMessages()
            ->build();
    }

    /** @test */
    public function the_message_timeout_should_be_zero(): void
    {
        $this->assertSame(0, $this->settings->messageTimeoutMilliseconds());
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function the_subscription_is_created_without_error(): void
    {
        $this->conn->createPersistentSubscription(
            $this->stream,
            'dont-timeout',
            $this->settings,
            DefaultData::adminCredentials()
        );
    }
}
