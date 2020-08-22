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

namespace ProophTest\EventStoreHttpClient\PersistentSubscriptionManagement;

use Http\Adapter\Guzzle6\Client;
use PHPUnit\Framework\TestCase;
use Prooph\EventStore\EventData;
use Prooph\EventStore\EventStorePersistentSubscription;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\PersistentSubscriptionNakEventAction;
use Prooph\EventStore\PersistentSubscriptions\PersistentSubscriptionDetails;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\Util\Guid;
use Prooph\EventStoreHttpClient\PersistentSubscriptions\PersistentSubscriptionsManager;
use Prooph\EventStoreHttpClient\PersistentSubscriptions\PersistentSubscriptionsManagerFactory;
use ProophTest\EventStoreHttpClient\DefaultData;
use ProophTest\EventStoreHttpClient\Helper\TestConnection;
use ProophTest\EventStoreHttpClient\SpecificationWithConnection;

class persistent_subscription_manager extends TestCase
{
    use SpecificationWithConnection;

    /** @var PersistentSubscriptionsManager */
    private $manager;
    /** @var string */
    private $stream;
    /** @var PersistentSubscriptionSettings */
    private $settings;
    /** @var EventStorePersistentSubscription */
    private $sub;

    protected function setUp(): void
    {
        $this->manager = PersistentSubscriptionsManagerFactory::create(
            TestConnection::settings(DefaultData::adminCredentials()),
            Client::createWithConfig([
                'verify' => false,
            ])
        );
        $this->stream = Guid::generateAsHex();
        $this->settings = PersistentSubscriptionSettings::create()
            ->doNotResolveLinkTos()
            ->startFromBeginning()
            ->build();
    }

    protected function when(): void
    {
        $this->conn->createPersistentSubscription(
            $this->stream,
            'existing',
            $this->settings,
            DefaultData::adminCredentials()
        );

        $this->sub = $this->conn->connectToPersistentSubscription(
            $this->stream,
            'existing',
            function (
                EventStorePersistentSubscription $subscription,
                ResolvedEvent $resolvedEvent,
                ?int $retryCount = null
            ): void {
                static $i = 0;
                $i++;

                if ($i === 2) {
                    $i = 0;
                    $subscription->stop();
                }
            },
            null,
            10,
            true,
            DefaultData::adminCredentials()
        );

        $this->conn->appendToStream(
            $this->stream,
            ExpectedVersion::ANY,
            [
                new EventData(null, 'whatever', true, \json_encode(['foo' => 2])),
                new EventData(null, 'whatever', true, \json_encode(['bar' => 3])),
            ]
        );
    }

    /** @test */
    public function can_describe_persistent_subscription(): void
    {
        $this->execute(function (): void {
            $this->sub->start();

            $details = $this->manager->describe($this->stream, 'existing');
            \assert($details instanceof PersistentSubscriptionDetails);

            $this->assertEquals($this->stream, $details->eventStreamId());
            $this->assertEquals('existing', $details->groupName());
            $this->assertEquals(2, $details->totalItemsProcessed());
            $this->assertEquals('Live', $details->status());
            $this->assertEquals(1, $details->lastKnownEventNumber());
        });
    }

    /** @test */
    public function cannot_describe_persistent_subscription_with_empty_stream_name(): void
    {
        $this->execute(function (): void {
            $this->expectException(InvalidArgumentException::class);
            $this->manager->describe('', 'existing');
        });
    }

    /** @test */
    public function cannot_describe_persistent_subscription_with_empty_group_name(): void
    {
        $this->execute(function (): void {
            $this->expectException(InvalidArgumentException::class);
            $this->manager->describe($this->stream, '');
        });
    }

    /** @test */
    public function can_list_all_persistent_subscriptions(): void
    {
        $this->execute(function (): void {
            $list = $this->manager->list();

            $found = false;
            foreach ($list as $details) {
                \assert($details instanceof PersistentSubscriptionDetails);
                if ($details->eventStreamId() === $this->stream
                    && $details->groupName() === 'existing'
                ) {
                    $found = true;
                    break;
                }
            }

            $this->assertTrue($found);
        });
    }

    /** @test */
    public function can_list_all_persistent_subscriptions_using_empty_string(): void
    {
        $this->execute(function (): void {
            $list = $this->manager->list('');

            $found = false;
            foreach ($list as $details) {
                \assert($details instanceof PersistentSubscriptionDetails);
                if ($details->eventStreamId() === $this->stream
                    && $details->groupName() === 'existing'
                ) {
                    $found = true;
                    break;
                }
            }

            $this->assertTrue($found);
        });
    }

    /** @test */
    public function can_list_persistent_subscriptions_for_stream(): void
    {
        $this->execute(function (): void {
            $list = $this->manager->list($this->stream);

            $found = false;
            foreach ($list as $details) {
                \assert($details instanceof PersistentSubscriptionDetails);

                $this->assertEquals($this->stream, $details->eventStreamId());

                if ($details->groupName() === 'existing') {
                    $found = true;
                    break;
                }
            }

            $this->assertTrue($found);
        });
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function can_replay_parked_messages(): void
    {
        global $s;
        $s = true;
        $this->execute(function (): void {
            $this->sub->start();
            $this->sub->stop();

            $this->sub = $this->conn->connectToPersistentSubscription(
                $this->stream,
                'existing',
                function (
                    EventStorePersistentSubscription $subscription,
                    ResolvedEvent $resolvedEvent,
                    ?int $retryCount = null
                ): void {
                    $subscription->fail(
                        $resolvedEvent,
                        PersistentSubscriptionNakEventAction::park(),
                        'testing'
                    );

                    static $i = 0;
                    $i++;

                    if ($i === 2) {
                        $subscription->stop();
                    }
                },
                null,
                10,
                false,
                DefaultData::adminCredentials()
            );

            $this->conn->appendToStream(
                $this->stream,
                ExpectedVersion::ANY,
                [
                    new EventData(null, 'whatever', true, \json_encode(['foo' => 2])),
                    new EventData(null, 'whatever', true, \json_encode(['bar' => 3])),
                ]
            );

            $this->sub->start();
            $this->sub->stop();

            $this->manager->replayParkedMessages($this->stream, 'existing', DefaultData::adminCredentials());

            $sub = $this->conn->connectToPersistentSubscription(
                $this->stream,
                'existing',
                function (
                    EventStorePersistentSubscription $subscription,
                    ResolvedEvent $resolvedEvent,
                    ?int $retryCount = null
                ): void {
                    $subscription->fail(
                        $resolvedEvent,
                        PersistentSubscriptionNakEventAction::park(),
                        'testing'
                    );

                    static $i = 0;
                    $i++;

                    if ($i === 2) {
                        $subscription->stop();
                    }
                },
                null,
                10,
                false,
                DefaultData::adminCredentials()
            );

            $sub->start();
        });
    }

    /** @test */
    public function cannot_replay_parked_with_empty_stream_name(): void
    {
        $this->execute(function (): void {
            $this->expectException(InvalidArgumentException::class);
            $this->manager->replayParkedMessages('', 'existing');
        });
    }

    /** @test */
    public function cannot_replay_parked_with_empty_group_name(): void
    {
        $this->execute(function (): void {
            $this->expectException(InvalidArgumentException::class);
            $this->manager->replayParkedMessages($this->stream, '');
        });
    }
}
