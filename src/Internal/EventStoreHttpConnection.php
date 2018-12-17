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
use Prooph\EventStoreHttpClient\AllEventsSlice;
use Prooph\EventStoreHttpClient\ClientOperations;
use Prooph\EventStoreHttpClient\Common\SystemEventTypes;
use Prooph\EventStoreHttpClient\Common\SystemStreams;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Prooph\EventStoreHttpClient\EventData;
use Prooph\EventStoreHttpClient\EventReadResult;
use Prooph\EventStoreHttpClient\EventReadStatus;
use Prooph\EventStoreHttpClient\EventStoreConnection;
use Prooph\EventStoreHttpClient\Exception\InvalidArgumentException;
use Prooph\EventStoreHttpClient\Exception\OutOfRangeException;
use Prooph\EventStoreHttpClient\Exception\UnexpectedValueException;
use Prooph\EventStoreHttpClient\ExpectedVersion;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Prooph\EventStoreHttpClient\PersistentSubscriptionSettings;
use Prooph\EventStoreHttpClient\Position;
use Prooph\EventStoreHttpClient\RawStreamMetadataResult;
use Prooph\EventStoreHttpClient\StreamEventsSlice;
use Prooph\EventStoreHttpClient\StreamMetadata;
use Prooph\EventStoreHttpClient\StreamMetadataResult;
use Prooph\EventStoreHttpClient\SystemSettings;
use Prooph\EventStoreHttpClient\UserCredentials;
use Prooph\EventStoreHttpClient\Util\Json;

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

    public function connectionSettings(): ConnectionSettings
    {
        return $this->settings;
    }

    public function deleteStream(
        string $stream,
        int $expectedVersion,
        bool $hardDelete = false,
        ?UserCredentials $userCredentials = null
    ): void {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        (new ClientOperations\DeleteStreamOperation())(
            $this->httpClient,
            $this->requestFactory,
            $this->uriFactory,
            $this->baseUri,
            $stream,
            $expectedVersion,
            $hardDelete,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $this->settings->requireMaster()
        );
    }

    /**
     * @param string $stream
     * @param int $expectedVersion
     * @param EventData[] $events
     * @param null|UserCredentials $userCredentials
     */
    public function appendToStream(
        string $stream,
        int $expectedVersion,
        array $events = [],
        ?UserCredentials $userCredentials = null
    ): void {
        if (empty($stream)) {
            throw new InvalidArgumentException('Stream cannot be empty');
        }

        (new ClientOperations\AppendToStreamOperation())(
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

    public function setStreamMetadata(
        string $stream,
        int $expectedMetaStreamVersion,
        ?StreamMetadata $metadata = null,
        ?UserCredentials $userCredentials = null
    ): void {
        $string = $metadata ? Json::encode($metadata) : '';

        $this->setRawStreamMetadata(
            $stream,
            $expectedMetaStreamVersion,
            $string,
            $userCredentials
        );
    }

    public function setRawStreamMetadata(
        string $stream,
        int $expectedMetaStreamVersion,
        string $metadata = '',
        ?UserCredentials $userCredentials = null
    ): void {
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

        (new ClientOperations\AppendToStreamOperation())(
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

    public function setSystemSettings(
        SystemSettings $settings,
        ?UserCredentials $userCredentials = null
    ): void {
        $this->appendToStream(
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
}
