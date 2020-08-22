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

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\EventData;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\Util\Guid;

class create_persistent_subscription_on_existing_stream extends TestCase
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
        $this->conn->appendToStream(
            $this->stream,
            ExpectedVersion::ANY,
            [new EventData(null, 'whatever', true, '{"foo":"bar"}')]
        );
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function the_completion_succeeds(): void
    {
        $this->execute(function () {
            $this->conn->createPersistentSubscription(
                $this->stream,
                'existing',
                $this->settings,
                DefaultData::adminCredentials()
            );
        });
    }
}
