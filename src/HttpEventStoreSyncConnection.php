<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2018 prooph software GmbH <contact@prooph.de>
 * (c) 2018-2018 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStoreHttpClient;

use Http\Client\HttpClient;
use Http\Message\RequestFactory;
use Http\Message\UriFactory;
use Prooph\EventStore\Common\SystemEventTypes;
use Prooph\EventStore\Common\SystemStreams;
use Prooph\EventStore\Data\DetailedSubscriptionInformation;
use Prooph\EventStore\Data\EventData;
use Prooph\EventStore\Data\EventId;
use Prooph\EventStore\Data\EventReadResult;
use Prooph\EventStore\Data\EventReadStatus;
use Prooph\EventStore\Data\ExpectedVersion;
use Prooph\EventStore\Data\PersistentSubscriptionSettings;
use Prooph\EventStore\Data\Position;
use Prooph\EventStore\Data\StreamEventsSlice;
use Prooph\EventStore\Data\StreamMetadata;
use Prooph\EventStore\Data\StreamMetadataResult;
use Prooph\EventStore\Data\SubscriptionInformation;
use Prooph\EventStore\Data\SystemSettings;
use Prooph\EventStore\Data\UserCredentials;
use Prooph\EventStore\Data\WriteResult;
use Prooph\EventStore\EventStorePersistentSubscription;
use Prooph\EventStore\EventStoreSyncConnection;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Exception\OutOfRangeException;
use Prooph\EventStore\Exception\UnexpectedValueException;
use Prooph\EventStore\Internal\Consts;
use Prooph\EventStore\Internal\Data\PersistentSubscriptionCreateResult;
use Prooph\EventStore\Internal\Data\PersistentSubscriptionDeleteResult;
use Prooph\EventStore\Internal\Data\PersistentSubscriptionUpdateResult;
use Prooph\EventStore\Internal\Data\ReplayParkedResult;
use Prooph\EventStoreHttpClient\ClientOperations\AppendToStreamOperation;
use Prooph\EventStoreHttpClient\ClientOperations\CreatePersistentSubscriptionOperation;
use Prooph\EventStoreHttpClient\ClientOperations\DeletePersistentSubscriptionOperation;
use Prooph\EventStoreHttpClient\ClientOperations\DeleteStreamOperation;
use Prooph\EventStoreHttpClient\ClientOperations\GetInformationForAllSubscriptionsOperation;
use Prooph\EventStoreHttpClient\ClientOperations\GetInformationForSubscriptionOperation;
use Prooph\EventStoreHttpClient\ClientOperations\GetInformationForSubscriptionsWithStreamOperation;
use Prooph\EventStoreHttpClient\ClientOperations\PersistentSubscriptionOperations;
use Prooph\EventStoreHttpClient\ClientOperations\ReadEventOperation;
use Prooph\EventStoreHttpClient\ClientOperations\ReadStreamEventsBackwardOperation;
use Prooph\EventStoreHttpClient\ClientOperations\ReadStreamEventsForwardOperation;
use Prooph\EventStoreHttpClient\ClientOperations\ReplayParkedOperation;
use Prooph\EventStoreHttpClient\ClientOperations\UpdatePersistentSubscriptionOperation;

class HttpEventStoreSyncConnection implements EventStoreSyncConnection
{
    // @todo add http interface? see replay parked, get information for subscriptions, no events handlers, etc.

    /** @var HttpClient */
    private $httpClient;
    /** @var RequestFactory */
    private $requestFactory;
    /** @var UriFactory */
    private $uriFactory;
    /** @var ConnectionSettings */
    private $settings;
    /** @var string */
    private $baseUri;

    public function __construct(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        ConnectionSettings $settings = null
    ) {
        $this->httpClient = $httpClient;
        $this->requestFactory = $requestFactory;
        $this->uriFactory = $uriFactory;
        $this->settings = $settings ?? ConnectionSettings::default();
        $this->baseUri = \sprintf(
            '%s://%s:%s',
            $this->settings->useSslConnection() ? 'https' : 'http',
            $this->settings->endPoint()->host(),
            $this->settings->endPoint()->port()
        );
    }

    public function connect(): void
    {
        // do nothing
    }

    public function close(): void
    {
        // do nothing
    }

    public function deleteStream(
        string $stream,
        int $expectedVersion,
        bool $hardDelete,
        UserCredentials $userCredentials = null
    ): void {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        // @todo add missing usage of $expectedVersion
        (new DeleteStreamOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $hardDelete,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    /**
     * @param string $stream
     * @param int $expectedVersion
     * @param null|UserCredentials $userCredentials
     * @param EventData[] $events
     * @return WriteResult
     */
    public function appendToStream(
        string $stream,
        int $expectedVersion,
        array $events,
        UserCredentials $userCredentials = null
    ): WriteResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        return (new AppendToStreamOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $expectedVersion,
            $events,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    public function readEvent(
        string $stream,
        int $eventNumber,
        bool $resolveLinkTo = true,
        UserCredentials $userCredentials = null
    ): EventReadResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if ('$all' === $stream) {
            throw new InvalidArgumentException('Stream cannot be $all');
        }

        if ($eventNumber < -1) {
            throw new InvalidArgumentException('EventNumber cannot be smaller then -1');
        }

        return (new ReadEventOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $eventNumber,
            $resolveLinkTo,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    public function readStreamEventsForward(
        string $stream,
        int $start,
        int $count,
        bool $resolveLinkTos = true,
        UserCredentials $userCredentials = null
    ): StreamEventsSlice {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if ($start < 0) {
            throw new InvalidArgumentException('Start cannot be negative');
        }

        if ($count < 0) {
            throw new InvalidArgumentException('Count cannot be negative');
        }

        if ($count > Consts::MaxReadSize) {
            throw new InvalidArgumentException(
                'Count should be less than ' . Consts::MaxReadSize . '. For larger reads you should page.'
            );
        }

        return (new ReadStreamEventsForwardOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $start,
            $count,
            $resolveLinkTos,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    public function readStreamEventsBackward(
        string $stream,
        int $start,
        int $count,
        bool $resolveLinkTos = true,
        UserCredentials $userCredentials = null
    ): StreamEventsSlice {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if ($start < 0) {
            throw new InvalidArgumentException('Start cannot be negative');
        }

        if ($count < 0) {
            throw new InvalidArgumentException('Count cannot be negative');
        }

        if ($count > Consts::MaxReadSize) {
            throw new InvalidArgumentException(
                'Count should be less than ' . Consts::MaxReadSize . '. For larger reads you should page.'
            );
        }

        return (new ReadStreamEventsBackwardOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $start,
            $count,
            $resolveLinkTos,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    public function readAllEventsForward(
        Position $position,
        int $count,
        bool $resolveLinkTos = true,
        UserCredentials $userCredentials = null
    ): StreamEventsSlice {
        // TODO: Implement readAllEventsForward() method.
    }

    public function readAllEventsBackward(
        Position $position,
        int $count,
        bool $resolveLinkTos = true,
        UserCredentials $userCredentials = null
    ): StreamEventsSlice {
        // TODO: Implement readAllEventsBackward() method.
    }

    public function setStreamMetadata(
        string $stream,
        int $expectedMetaStreamVersion,
        StreamMetadata $metadata,
        UserCredentials $userCredentials = null
    ): WriteResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (SystemStreams::isMetastream($stream)) {
            throw new InvalidArgumentException(\sprintf(
                'Setting metadata for metastream \'%s\' is not supported.',
                $stream
            ));
        }

        $metaEvent = new EventData(
            EventId::generate(),
            SystemEventTypes::StreamMetadata,
            true,
            \json_encode($metadata->toArray()),
            ''
        );

        return (new AppendToStreamOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            SystemStreams::metastreamOf($stream),
            $expectedMetaStreamVersion,
            [$metaEvent],
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    public function getStreamMetadata(string $stream, UserCredentials $userCredentials = null): StreamMetadataResult
    {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        $eventReadResult = $this->readEvent(
            SystemStreams::metastreamOf($stream),
            -1,
            false,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );

        switch ($eventReadResult->status()->value()) {
            case EventReadStatus::Success:
                $event = $eventReadResult->event();

                if (null === $event) {
                    throw new UnexpectedValueException('Event is null while operation result is Success');
                }

                return new StreamMetadataResult(
                    $stream,
                    false,
                    $event->eventNumber(),
                    $event->data()
                );
            case EventReadStatus::NotFound:
            case EventReadStatus::NoStream:
                return new StreamMetadataResult($stream, false, -1, '');
            case EventReadStatus::StreamDeleted:
                return new StreamMetadataResult($stream, true, PHP_INT_MAX, '');
            default:
                throw new OutOfRangeException('Unexpected ReadEventResult: ' . $eventReadResult->status()->value());
        }
    }

    public function setSystemSettings(SystemSettings $settings, UserCredentials $userCredentials = null): WriteResult
    {
        return $this->appendToStream(
            SystemStreams::SettingsStream,
            ExpectedVersion::Any,
            [
                new EventData(
                    EventId::generate(),
                    SystemEventTypes::Settings,
                    true,
                    \json_encode($settings->toArray()),
                    ''
                ),
            ],
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    public function createPersistentSubscription(
        string $stream,
        string $groupName,
        PersistentSubscriptionSettings $settings,
        UserCredentials $userCredentials = null
    ): PersistentSubscriptionCreateResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($groupName)) {
            throw new InvalidArgumentException('Group name cannot be empty');
        }

        return (new CreatePersistentSubscriptionOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $groupName,
            $settings,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    public function updatePersistentSubscription(
        string $stream,
        string $groupName,
        PersistentSubscriptionSettings $settings,
        UserCredentials $userCredentials = null
    ): PersistentSubscriptionUpdateResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($groupName)) {
            throw new InvalidArgumentException('Group name cannot be empty');
        }

        return (new UpdatePersistentSubscriptionOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $groupName,
            $settings,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    public function deletePersistentSubscription(
        string $stream,
        string $groupName,
        UserCredentials $userCredentials = null
    ): PersistentSubscriptionDeleteResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($groupName)) {
            throw new InvalidArgumentException('Group name cannot be empty');
        }

        return (new DeletePersistentSubscriptionOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $groupName,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    /**
     * @param string $stream
     * @param string $groupName
     * @param callable(EventStorePersistentSubscription $subscription, RecordedEvent $event, int $retryCount, Task $task) $eventAppeared
     * @param callable(EventStorePersistentSubscription $subscription, SubscriptionDropReason $reason, Throwable $error)|null $subscriptionDropped
     * @param int $bufferSize
     * @param bool $autoAck
     * @param bool $autoNack
     * @param UserCredentials|null $userCredentials
     * @return EventStorePersistentSubscription
     */
    public function connectToPersistentSubscription(
        string $stream,
        string $groupName,
        callable $eventAppeared,
        callable $subscriptionDropped = null,
        int $bufferSize = 10,
        bool $autoAck = true,
        UserCredentials $userCredentials = null
    ): EventStorePersistentSubscription {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($groupName)) {
            throw new InvalidArgumentException('Group name cannot be empty');
        }

        return new EventStorePersistentSubscription(
            new PersistentSubscriptionOperations(
                $this->httpClient,
                $this->requestFactory,
                $this->uriFactory,
                $this->baseUri,
                $stream,
                $groupName,
                $userCredentials ?? $this->settings->defaultUserCredentials()
            ),
            $groupName,
            $stream,
            $eventAppeared,
            $subscriptionDropped,
            $bufferSize,
            $autoAck
        );
    }

    public function replayParked(
        string $stream,
        string $groupName,
        UserCredentials $userCredentials = null
    ): ReplayParkedResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($groupName)) {
            throw new InvalidArgumentException('Group name cannot be empty');
        }

        return (new ReplayParkedOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $groupName,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    /**
     * @return SubscriptionInformation[]
     */
    public function getInformationForAllSubscriptions(
        UserCredentials $userCredentials = null
    ): array {
        return (new GetInformationForAllSubscriptionsOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    /**
     * @return SubscriptionInformation[]
     */
    public function getInformationForSubscriptionsWithStream(
        string $stream,
        UserCredentials $userCredentials = null
    ): array {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        return (new GetInformationForSubscriptionsWithStreamOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }

    public function getInformationForSubscription(
        string $stream,
        string $groupName,
        UserCredentials $userCredentials = null
    ): DetailedSubscriptionInformation {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($groupName)) {
            throw new InvalidArgumentException('Group name cannot be empty');
        }

        return (new GetInformationForSubscriptionOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $groupName,
            $userCredentials ?? $this->settings->defaultUserCredentials()
        );
    }
}
