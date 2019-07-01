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
use Prooph\EventStore\Exception\InvalidOperationException;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\Util\Guid;

class create_duplicate_persistent_subscription_group extends TestCase
{
    use SpecificationWithConnection;

    /** @var string */
    private $stream;
    /** @var PersistentSubscriptionSettings */
    private $settings;

    protected function setUp(): void
    {
        $this->stream = Guid::generateAsHex();
        $this->settings = PersistentSubscriptionSettings::create()
            ->doNotResolveLinkTos()
            ->startFromCurrent()
            ->build();
    }

    protected function when(): void
    {
        $this->conn->createPersistentSubscription(
            $this->stream,
            'group32',
            $this->settings,
            DefaultData::adminCredentials()
        );
    }

    /** @test */
    public function the_completion_fails_with_invalid_operation_exception(): void
    {
        $this->execute(function (): void {
            $this->expectException(InvalidOperationException::class);

            $this->conn->createPersistentSubscription(
                $this->stream,
                'group32',
                $this->settings,
                DefaultData::adminCredentials()
            );
        });
    }
}
