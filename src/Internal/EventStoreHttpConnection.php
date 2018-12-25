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

namespace Prooph\EventStoreHttpClient\Internal;

use Http\Message\RequestFactory;
use Http\Message\UriFactory;
use Prooph\EventStore\AllEventsSlice;
use Prooph\EventStore\CatchUpSubscriptionDropped;
use Prooph\EventStore\CatchUpSubscriptionSettings;
use Prooph\EventStore\Common\SystemEventTypes;
use Prooph\EventStore\Common\SystemStreams;
use Prooph\EventStore\ConditionalWriteResult;
use Prooph\EventStore\ConnectionSettings as BaseConnectionSettings;
use Prooph\EventStore\DeleteResult;
use Prooph\EventStore\EventAppearedOnCatchupSubscription;
use Prooph\EventStore\EventAppearedOnPersistentSubscription;
use Prooph\EventStore\EventAppearedOnSubscription;
use Prooph\EventStore\EventData;
use Prooph\EventStore\EventReadResult;
use Prooph\EventStore\EventReadStatus;
use Prooph\EventStore\EventStoreAllCatchUpSubscription;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\EventStorePersistentSubscription;
use Prooph\EventStore\EventStoreStreamCatchUpSubscription;
use Prooph\EventStore\EventStoreSubscription;
use Prooph\EventStore\EventStoreTransaction;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Exception\InvalidOperationException;
use Prooph\EventStore\Exception\OutOfRangeException;
use Prooph\EventStore\Exception\UnexpectedValueException;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\Internal\Consts;
use Prooph\EventStore\Internal\PersistentSubscriptionCreateResult;
use Prooph\EventStore\Internal\PersistentSubscriptionDeleteResult;
use Prooph\EventStore\Internal\PersistentSubscriptionUpdateResult;
use Prooph\EventStore\LiveProcessingStartedOnCatchUpSubscription;
use Prooph\EventStore\PersistentSubscriptionDropped;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\Position;
use Prooph\EventStore\RawStreamMetadataResult;
use Prooph\EventStore\StreamEventsSlice;
use Prooph\EventStore\StreamMetadata;
use Prooph\EventStore\StreamMetadataResult;
use Prooph\EventStore\SubscriptionDropped;
use Prooph\EventStore\SystemSettings;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStore\Util\Json;
use Prooph\EventStore\WriteResult;
use Prooph\EventStoreHttpClient\ClientOperations;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Prooph\EventStoreHttpClient\Http\HttpClient;

/** @internal */
class EventStoreHttpConnection implements EventStoreConnection
{
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

    /** @internal */
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
            $this->settings->schema(),
            $this->settings->endPoint()->host(),
            $this->settings->endPoint()->port()
        );
    }

    public function connectionSettings(): BaseConnectionSettings
    {
        return $this->settings;
    }

    /**
     * Note: The `DeleteResult` will always contain an invalid `Position`.
     *
     * @param string $stream
     * @param int $expectedVersion
     * @param bool $hardDelete
     * @param UserCredentials|null $userCredentials
     *
     * @return DeleteResult
     */
    public function deleteStream(
        string $stream,
        int $expectedVersion,
        bool $hardDelete = false,
        ?UserCredentials $userCredentials = null
    ): DeleteResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        return (new ClientOperations\DeleteStreamOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $expectedVersion,
            false,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    /**
     * Note: The `WriteResult` will always contain ExpectedVersion::ANY with an invalid `Position`
     *
     * @param string $stream
     * @param int $expectedVersion
     * @param EventData[] $events
     * @param null|UserCredentials $userCredentials
     *
     * @return WriteResult
     */
    public function appendToStream(
        string $stream,
        int $expectedVersion,
        array $events = [],
        ?UserCredentials $userCredentials = null
    ): WriteResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        return (new ClientOperations\AppendToStreamOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $expectedVersion,
            $events,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function conditionalAppendToStream(
        string $stream,
        int $expectedVersion,
        array $events = [],
        ?UserCredentials $userCredentials = null
    ): ConditionalWriteResult {
        throw new InvalidOperationException('Not implemented on HTTP client');
    }

    public function readEvent(
        string $stream,
        int $eventNumber,
        bool $resolveLinkTos = true,
        ?UserCredentials $userCredentials = null
    ): EventReadResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if ('$all' === $stream) {
            throw new InvalidArgumentException('Stream cannot be $all');
        }

        if ($eventNumber < -1) {
            throw new OutOfRangeException('Event number is out of range');
        }

        return (new ClientOperations\ReadEventOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $eventNumber,
            $resolveLinkTos,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function readStreamEventsForward(
        string $stream,
        int $start,
        int $count,
        bool $resolveLinkTos = true,
        ?UserCredentials $userCredentials = null
    ): StreamEventsSlice {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if ($start < 0) {
            throw new InvalidArgumentException('Start must be positive');
        }

        if ($count < 1) {
            throw new InvalidArgumentException('Count must be positive');
        }

        if ($count > Consts::MAX_READ_SIZE) {
            throw new InvalidArgumentException(\sprintf(
                'Count should be less than %s. For larger reads you should page.',
                Consts::MAX_READ_SIZE
            ));
        }

        return (new ClientOperations\ReadStreamEventsForwardOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $start,
            $count,
            $resolveLinkTos,
            0,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function readStreamEventsForwardPolling(
        string $stream,
        int $start,
        int $count,
        bool $resolveLinkTos = true,
        int $longPoll = 0,
        ?UserCredentials $userCredentials = null
    ): StreamEventsSlice {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if ($start < 0) {
            throw new InvalidArgumentException('Start must be positive');
        }

        if ($count < 1) {
            throw new InvalidArgumentException('Count must be positive');
        }

        if ($count > Consts::MAX_READ_SIZE) {
            throw new InvalidArgumentException(\sprintf(
                'Count should be less than %s. For larger reads you should page.',
                Consts::MAX_READ_SIZE
            ));
        }

        return (new ClientOperations\ReadStreamEventsForwardOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $start,
            $count,
            $resolveLinkTos,
            $longPoll,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function readStreamEventsBackward(
        string $stream,
        int $start,
        int $count,
        bool $resolveLinkTos = true,
        ?UserCredentials $userCredentials = null
    ): StreamEventsSlice {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if ($count < 1) {
            throw new InvalidArgumentException('Count must be positive');
        }

        if ($count > Consts::MAX_READ_SIZE) {
            throw new InvalidArgumentException(\sprintf(
                'Count should be less than %s. For larger reads you should page.',
                Consts::MAX_READ_SIZE
            ));
        }

        return (new ClientOperations\ReadStreamEventsBackwardOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $start,
            $count,
            $resolveLinkTos,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function readAllEventsForward(
        Position $position,
        int $count,
        bool $resolveLinkTos = true,
        ?UserCredentials $userCredentials = null
    ): AllEventsSlice {
        if ($count < 1) {
            throw new InvalidArgumentException('Count must be positive');
        }

        if ($count > Consts::MAX_READ_SIZE) {
            throw new InvalidArgumentException(\sprintf(
                'Count should be less than %s. For larger reads you should page.',
                Consts::MAX_READ_SIZE
            ));
        }

        return (new ClientOperations\ReadAllEventsForwardOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $position,
            $count,
            $resolveLinkTos,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function readAllEventsBackward(
        Position $position,
        int $count,
        bool $resolveLinkTos = true,
        ?UserCredentials $userCredentials = null
    ): AllEventsSlice {
        if ($count < 1) {
            throw new InvalidArgumentException('Count must be positive');
        }

        if ($count > Consts::MAX_READ_SIZE) {
            throw new InvalidArgumentException(\sprintf(
                'Count should be less than %s. For larger reads you should page.',
                Consts::MAX_READ_SIZE
            ));
        }

        return (new ClientOperations\ReadAllEventsBackwardOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $position,
            $count,
            $resolveLinkTos,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    /**
     * Note: The `WriteResult` will always contain ExpectedVersion::ANY with an invalid `Position`
     *
     * @param string $stream
     * @param int $expectedMetaStreamVersion
     * @param StreamMetadata|null $metadata
     * @param UserCredentials|null $userCredentials
     *
     * @return WriteResult
     */
    public function setStreamMetadata(
        string $stream,
        int $expectedMetaStreamVersion,
        ?StreamMetadata $metadata = null,
        ?UserCredentials $userCredentials = null
    ): WriteResult {
        $string = $metadata ? Json::encode($metadata) : '';

        return $this->setRawStreamMetadata(
            $stream,
            $expectedMetaStreamVersion,
            $string,
            $userCredentials
        );
    }

    /**
     * Note: The `WriteResult` will always contain ExpectedVersion::ANY with an invalid `Position`
     *
     * @param string $stream
     * @param int $expectedMetaStreamVersion
     * @param string $metadata
     * @param UserCredentials|null $userCredentials
     *
     * @return WriteResult
     */
    public function setRawStreamMetadata(
        string $stream,
        int $expectedMetaStreamVersion,
        string $metadata = '',
        ?UserCredentials $userCredentials = null
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
            null,
            SystemEventTypes::STREAM_METADATA,
            true,
            $metadata
        );

        return (new ClientOperations\AppendToStreamOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $expectedMetaStreamVersion,
            [$metaEvent],
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function getStreamMetadata(
        string $stream,
        ?UserCredentials $userCredentials = null
    ): StreamMetadataResult {
        $result = $this->getRawStreamMetadata($stream, $userCredentials);

        if ($result->streamMetadata() === '') {
            return new StreamMetadataResult(
                $result->stream(),
                $result->isStreamDeleted(),
                $result->metastreamVersion(),
                new StreamMetadata()
            );
        }

        $metadata = StreamMetadata::createFromArray(Json::decode($result->streamMetadata()));

        return new StreamMetadataResult(
            $result->stream(),
            $result->isStreamDeleted(),
            $result->metastreamVersion(),
            $metadata
        );
    }

    public function getRawStreamMetadata(
        string $stream,
        ?UserCredentials $userCredentials = null
    ): RawStreamMetadataResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        $eventReadResult = $this->readEvent(
            SystemStreams::metastreamOf($stream),
            -1,
            false,
            $userCredentials
        );

        switch ($eventReadResult->status()->value()) {
            case EventReadStatus::SUCCESS:
                $event = $eventReadResult->event();

                if (null === $event) {
                    throw new UnexpectedValueException('Event is null while operation result is Success');
                }

                $event = $event->originalEvent();

                if (null === $event) {
                    return new RawStreamMetadataResult(
                        $stream,
                        false,
                        -1,
                        ''
                    );
                }

                return new RawStreamMetadataResult(
                    $stream,
                    false,
                    $event->eventNumber(),
                    $event->data()
                );
            case EventReadStatus::NOT_FOUND:
            case EventReadStatus::NO_STREAM:
                return new RawStreamMetadataResult($stream, false, -1, '');
            case EventReadStatus::STREAM_DELETED:
                return new RawStreamMetadataResult($stream, true, \PHP_INT_MAX, '');
            default:
                throw new OutOfRangeException(\sprintf(
                    'Unexpected ReadEventResult: %s',
                    $eventReadResult->status()->name()
                ));
        }
    }

    /**
     * Note: The `WriteResult` will always contain ExpectedVersion::ANY with an invalid `Position`
     *
     * @param SystemSettings $settings
     * @param UserCredentials|null $userCredentials
     *
     * @return WriteResult
     */
    public function setSystemSettings(
        SystemSettings $settings,
        ?UserCredentials $userCredentials = null
    ): WriteResult {
        return $this->appendToStream(
            SystemStreams::SETTINGS_STREAM,
            ExpectedVersion::ANY,
            [new EventData(null, SystemEventTypes::SETTINGS, true, Json::encode($settings))],
            $userCredentials
        );
    }

    public function createPersistentSubscription(
        string $stream,
        string $groupName,
        PersistentSubscriptionSettings $settings,
        ?UserCredentials $userCredentials = null
    ): PersistentSubscriptionCreateResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($groupName)) {
            throw new InvalidArgumentException('Group name cannot be empty');
        }

        return (new ClientOperations\CreatePersistentSubscriptionOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $groupName,
            $settings,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function updatePersistentSubscription(
        string $stream,
        string $groupName,
        PersistentSubscriptionSettings $settings,
        ?UserCredentials $userCredentials = null
    ): PersistentSubscriptionUpdateResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($groupName)) {
            throw new InvalidArgumentException('Group name cannot be empty');
        }

        return (new ClientOperations\UpdatePersistentSubscriptionOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $groupName,
            $settings,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function deletePersistentSubscription(
        string $stream,
        string $groupName,
        ?UserCredentials $userCredentials = null
    ): PersistentSubscriptionDeleteResult {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        if (empty($groupName)) {
            throw new InvalidArgumentException('Group name cannot be empty');
        }

        return (new ClientOperations\DeletePersistentSubscriptionOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $groupName,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    public function startTransaction(
        string $stream,
        int $expectedVersion,
        ?UserCredentials $userCredentials = null
    ): EventStoreTransaction {
        throw new InvalidOperationException('Not implemented on HTTP client');
    }

    public function continueTransaction(
        int $transactionId,
        ?UserCredentials $userCredentials = null
    ): EventStoreTransaction {
        throw new InvalidOperationException('Not implemented on HTTP client');
    }

    public function subscribeToStreamAsync(
        string $stream,
        bool $resolveLinkTos,
        EventAppearedOnSubscription $eventAppeared,
        ?SubscriptionDropped $subscriptionDropped = null,
        ?UserCredentials $userCredentials = null
    ): EventStoreSubscription {
        // TODO: Implement subscribeToStreamAsync() method.
    }

    public function subscribeToStreamFromAsync(
        string $stream,
        ?int $lastCheckpoint,
        ?CatchUpSubscriptionSettings $settings,
        EventAppearedOnCatchupSubscription $eventAppeared,
        ?LiveProcessingStartedOnCatchUpSubscription $liveProcessingStarted = null,
        ?CatchUpSubscriptionDropped $subscriptionDropped = null,
        ?UserCredentials $userCredentials = null
    ): EventStoreStreamCatchUpSubscription {
        // TODO: Implement subscribeToStreamFromAsync() method.
    }

    public function subscribeToAllAsync(
        bool $resolveLinkTos,
        EventAppearedOnSubscription $eventAppeared,
        ?SubscriptionDropped $subscriptionDropped = null,
        ?UserCredentials $userCredentials = null
    ): EventStoreSubscription {
        // TODO: Implement subscribeToAllAsync() method.
    }

    public function subscribeToAllFromAsync(
        ?Position $lastCheckpoint,
        ?CatchUpSubscriptionSettings $settings,
        EventAppearedOnCatchupSubscription $eventAppeared,
        ?LiveProcessingStartedOnCatchUpSubscription $liveProcessingStarted = null,
        ?CatchUpSubscriptionDropped $subscriptionDropped = null,
        ?UserCredentials $userCredentials = null
    ): EventStoreAllCatchUpSubscription {
        // TODO: Implement subscribeToAllFromAsync() method.
    }

    public function connectToPersistentSubscription(
        string $stream,
        string $groupName,
        EventAppearedOnPersistentSubscription $eventAppeared,
        ?PersistentSubscriptionDropped $subscriptionDropped = null,
        int $bufferSize = 10,
        bool $autoAck = true,
        ?UserCredentials $userCredentials = null
    ): EventStorePersistentSubscription {
        // TODO: Implement connectToPersistentSubscription() method.
    }
}
