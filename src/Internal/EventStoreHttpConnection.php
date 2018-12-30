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
use Prooph\EventStore\AllEventsSlice;
use Prooph\EventStore\CatchUpSubscriptionDropped;
use Prooph\EventStore\CatchUpSubscriptionSettings;
use Prooph\EventStore\Common\SystemEventTypes;
use Prooph\EventStore\Common\SystemStreams;
use Prooph\EventStore\ConditionalWriteResult;
use Prooph\EventStore\DeleteResult;
use Prooph\EventStore\EventAppearedOnCatchupSubscription;
use Prooph\EventStore\EventAppearedOnPersistentSubscription;
use Prooph\EventStore\EventAppearedOnSubscription;
use Prooph\EventStore\EventData;
use Prooph\EventStore\EventId;
use Prooph\EventStore\EventReadResult;
use Prooph\EventStore\EventReadStatus;
use Prooph\EventStore\EventStoreAllCatchUpSubscription;
use Prooph\EventStore\EventStoreConnection;
use Prooph\EventStore\EventStorePersistentSubscription;
use Prooph\EventStore\EventStoreStreamCatchUpSubscription;
use Prooph\EventStore\EventStoreSubscription;
use Prooph\EventStore\EventStoreTransaction;
use Prooph\EventStore\Exception\AccessDeniedException;
use Prooph\EventStore\Exception\EventStoreConnectionException;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Exception\InvalidOperationException;
use Prooph\EventStore\Exception\OutOfRangeException;
use Prooph\EventStore\Exception\StreamDeletedException;
use Prooph\EventStore\Exception\UnexpectedValueException;
use Prooph\EventStore\Exception\WrongExpectedVersionException;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\Internal\Consts;
use Prooph\EventStore\Internal\PersistentSubscriptionCreateResult;
use Prooph\EventStore\Internal\PersistentSubscriptionCreateStatus;
use Prooph\EventStore\Internal\PersistentSubscriptionDeleteResult;
use Prooph\EventStore\Internal\PersistentSubscriptionDeleteStatus;
use Prooph\EventStore\Internal\PersistentSubscriptionUpdateResult;
use Prooph\EventStore\Internal\PersistentSubscriptionUpdateStatus;
use Prooph\EventStore\LiveProcessingStartedOnCatchUpSubscription;
use Prooph\EventStore\PersistentSubscriptionDropped;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\Position;
use Prooph\EventStore\RawStreamMetadataResult;
use Prooph\EventStore\ReadDirection;
use Prooph\EventStore\RecordedEvent;
use Prooph\EventStore\ResolvedEvent;
use Prooph\EventStore\SliceReadStatus;
use Prooph\EventStore\StreamEventsSlice;
use Prooph\EventStore\StreamMetadata;
use Prooph\EventStore\StreamMetadataResult;
use Prooph\EventStore\SubscriptionDropped;
use Prooph\EventStore\SystemSettings;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStore\Util\DateTime;
use Prooph\EventStore\Util\Json;
use Prooph\EventStore\WriteResult;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Psr\Http\Client\ClientInterface;
use Throwable;

/** @internal */
class EventStoreHttpConnection implements EventStoreConnection
{
    /** @var HttpClient */
    private $httpClient;
    /** @var ConnectionSettings */
    private $settings;
    /** @var callable */
    private $onException;
    /** @var string */
    private $baseUri;

    /** @internal */
    public function __construct(
        ClientInterface $httpClient,
        RequestFactory $requestFactory,
        ConnectionSettings $settings
    ) {
        $this->baseUri = \sprintf(
            '%s://%s:%s',
            $this->settings->schema(),
            $this->settings->endPoint()->host(),
            $this->settings->endPoint()->port()
        );

        $this->settings = $settings;

        $this->httpClient = new HttpClient(
            $httpClient,
            $requestFactory,
            $settings,
            $this->baseUri
        );

        $this->onException = static function (Throwable $e) {
            throw new EventStoreConnectionException($e->getMessage());
        };
    }

    public function connectionSettings(): ConnectionSettings
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

        $headers = [
            'ES-ExpectedVersion' => $expectedVersion,
        ];

        if ($hardDelete) {
            $headers['ES-HardDelete'] = 'true';
        }

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $response = $this->httpClient->delete(
            '/streams/' . \urlencode($stream),
            $headers,
            $userCredentials,
            $this->onException
        );

        switch ($response->getStatusCode()) {
            case 204:
            case 410:
                return new DeleteResult(Position::invalid());
            case 400:
                throw WrongExpectedVersionException::with($stream, $expectedVersion);
            case 401:
                throw AccessDeniedException::toStream($stream);
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
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

        $data = [];

        foreach ($events as $event) {
            \assert($event instanceof EventData);

            $data[] = [
                'eventId' => $event->eventId()->toString(),
                'eventType' => $event->eventType(),
                'data' => $event->data(),
                'metadata' => $event->metaData(),
            ];
        }

        $body = Json::encode($data);

        $headers = [
            'Content-Type' => 'application/vnd.eventstore.events+json',
            'Content-Length' => \strlen($body),
            'ES-ExpectedVersion' => $expectedVersion,
        ];

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $response = $this->httpClient->post(
            '/streams/' . \urlencode($stream),
            $headers,
            $body,
            $userCredentials,
            $this->onException
        );

        switch ($response->getStatusCode()) {
            case 201:
                return new WriteResult(ExpectedVersion::ANY, Position::invalid());
            case 400:
                $header = $response->getHeader('ES-CurrentVersion');

                if (empty($header)) {
                    throw new EventStoreConnectionException($response->getReasonPhrase());
                }

                $currentVersion = (int) $header[0];

                throw WrongExpectedVersionException::with($stream, $expectedVersion, $currentVersion);
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 410:
                throw StreamDeletedException::with($stream);
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
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

        $headers = [
            'Accept' => 'application/vnd.eventstore.atom+json',
        ];

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        if (-1 === $eventNumber) {
            $eventNumber = 'head';
        }

        $response = $this->httpClient->get(
            '/streams/' . \urlencode($stream) . '/' . $eventNumber . '?embed=tryharder',
            $headers,
            $userCredentials,
            $this->onException
        );

        switch ($response->getStatusCode()) {
            case 200:
                $json = Json::decode($response->getBody()->getContents());

                if (empty($json)) {
                    return new EventReadResult(EventReadStatus::notFound(), $stream, $eventNumber, null);
                }

                if ($resolveLinkTos && $json['streamId'] !== $stream) {
                    $data = $json['data'] ?? '';

                    if (\is_array($data)) {
                        $data = Json::encode($data);
                    }

                    $field = isset($json['isLinkMetaData']) && $json['isLinkMetaData'] ? 'linkMetaData' : 'metaData';

                    $metadata = $json[$field] ?? '';

                    if (\is_array($metadata)) {
                        $metadata = Json::encode($metadata);
                    }

                    $link = new RecordedEvent(
                        $stream,
                        $json['positionEventNumber'],
                        EventId::fromString($json['eventId']),
                        $json['eventType'],
                        $json['isJson'],
                        $data,
                        $metadata,
                        DateTime::create($json['updated'])
                    );
                } else {
                    $link = null;
                }

                $record = new RecordedEvent(
                    $json['streamId'],
                    $json['eventNumber'],
                    EventId::fromString($json['eventId']),
                    SystemEventTypes::LINK_TO,
                    false,
                    $json['title'],
                    '',
                    DateTime::create($json['updated'])
                );

                $event = new ResolvedEvent($record, $link, null);

                return new EventReadResult(EventReadStatus::success(), $stream, $eventNumber, $event);
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 404:
                return new EventReadResult(EventReadStatus::notFound(), $stream, $eventNumber, null);
            case 410:
                return new EventReadResult(EventReadStatus::streamDeleted(), $stream, $eventNumber, null);
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
    }

    public function readStreamEventsForward(
        string $stream,
        int $start,
        int $count,
        bool $resolveLinkTos = true,
        ?UserCredentials $userCredentials = null
    ): StreamEventsSlice {
        return $this->readStreamEventsForwardPolling(
            $stream,
            $start,
            $count,
            $resolveLinkTos,
            0,
            $userCredentials
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

        $headers = [
            'Accept' => 'application/vnd.eventstore.atom+json',
        ];

        if (! $resolveLinkTos) {
            $headers['ES-ResolveLinkTos'] = 'false';
        }

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        if ($longPoll > 0) {
            $headers['ES-LongPoll'] = $longPoll;
        }

        $response = $this->httpClient->get(
            '/streams/' . \urlencode($stream) . '/' . $start . '/forward/' . $count . '?embed=tryharder',
            $headers,
            $userCredentials,
            $this->onException
        );

        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 404:
                return new StreamEventsSlice(
                    SliceReadStatus::streamNotFound(),
                    $stream,
                    $start,
                    ReadDirection::forward(),
                    [],
                    0,
                    0,
                    true
                );
            case 410:
                return new StreamEventsSlice(
                    SliceReadStatus::streamDeleted(),
                    $stream,
                    $start,
                    ReadDirection::forward(),
                    [],
                    0,
                    0,
                    true
                );
            case 200:
                $json = Json::decode($response->getBody()->getContents());

                $events = [];
                $lastEventNumber = 0;
                foreach (\array_reverse($json['entries']) as $entry) {
                    if ($json['streamId'] !== $stream) {
                        $data = $entry['data'] ?? '';

                        if (\is_array($data)) {
                            $data = Json::encode($data);
                        }

                        $field = isset($json['isLinkMetaData']) && $json['isLinkMetaData'] ? 'linkMetaData' : 'metaData';

                        $metadata = $json[$field] ?? '';

                        if (\is_array($metadata)) {
                            $metadata = Json::encode($metadata);
                        }

                        $link = new RecordedEvent(
                            $stream,
                            $json['positionEventNumber'],
                            EventId::fromString($json['eventId']),
                            $json['eventType'],
                            $json['isJson'],
                            $data,
                            $metadata,
                            DateTime::create($json['updated'])
                        );
                    } else {
                        $link = null;
                    }

                    $record = new RecordedEvent(
                        $json['streamId'],
                        $json['eventNumber'],
                        EventId::fromString($json['eventId']),
                        SystemEventTypes::LINK_TO,
                        false,
                        $json['title'],
                        '',
                        DateTime::create($json['updated'])
                    );

                    $events[] = new ResolvedEvent($record, $link, null);

                    $lastEventNumber = $entry['eventNumber'];
                }

                return new StreamEventsSlice(
                    SliceReadStatus::success(),
                    $stream,
                    $start,
                    ReadDirection::forward(),
                    $events,
                    $lastEventNumber + 1,
                    $lastEventNumber,
                    $json['headOfStream']
                );
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
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

        $headers = [
            'Accept' => 'application/vnd.eventstore.atom+json',
        ];

        if (! $resolveLinkTos) {
            $headers['ES-ResolveLinkTos'] = 'false';
        }

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $response = $this->httpClient->get(
            '/streams/' . \urlencode($stream) . '/' . $start . '/backward/' . $count . '?embed=tryharder',
            $headers,
            $userCredentials,
            $this->onException
        );

        switch ($response->getStatusCode()) {
            case 200:
                $json = Json::decode($response->getBody()->getContents());

                $events = [];
                $lastEventNumber = 0;
                foreach ($json['entries'] as $entry) {
                    if ($json['streamId'] !== $stream) {
                        $data = $entry['data'] ?? '';

                        if (\is_array($data)) {
                            $data = Json::encode($data);
                        }

                        $field = isset($json['isLinkMetaData']) && $json['isLinkMetaData'] ? 'linkMetaData' : 'metaData';

                        $metadata = $json[$field] ?? '';

                        if (\is_array($metadata)) {
                            $metadata = Json::encode($metadata);
                        }

                        $link = new RecordedEvent(
                            $stream,
                            $json['positionEventNumber'],
                            EventId::fromString($json['eventId']),
                            $json['eventType'],
                            $json['isJson'],
                            $data,
                            $metadata,
                            DateTime::create($json['updated'])
                        );
                    } else {
                        $link = null;
                    }

                    $record = new RecordedEvent(
                        $json['streamId'],
                        $json['eventNumber'],
                        EventId::fromString($json['eventId']),
                        SystemEventTypes::LINK_TO,
                        false,
                        $json['title'],
                        '',
                        DateTime::create($json['updated'])
                    );

                    $events[] = new ResolvedEvent($record, $link, null);

                    $lastEventNumber = $entry['eventNumber'];
                }
                $nextEventNumber = ($lastEventNumber < 1) ? 0 : ($lastEventNumber - 1);

                return new StreamEventsSlice(
                    SliceReadStatus::success(),
                    $stream,
                    $start,
                    ReadDirection::backward(),
                    $events,
                    $nextEventNumber,
                    $lastEventNumber,
                    false
                );
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 404:
                return new StreamEventsSlice(
                    SliceReadStatus::streamNotFound(),
                    $stream,
                    $start,
                    ReadDirection::backward(),
                    [],
                    0,
                    0,
                    true
                );
            case 410:
                return new StreamEventsSlice(
                    SliceReadStatus::streamDeleted(),
                    $stream,
                    $start,
                    ReadDirection::backward(),
                    [],
                    0,
                    0,
                    true
                );
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
    }

    public function readAllEventsForward(
        Position $position,
        int $count,
        bool $resolveLinkTos = true,
        ?UserCredentials $userCredentials = null
    ): AllEventsSlice {
        return $this->readAllEventsForwardPolling(
            $position,
            $count,
            $resolveLinkTos,
            0,
            $userCredentials
        );
    }

    public function readAllEventsForwardPolling(
        Position $position,
        int $count,
        bool $resolveLinkTos = true,
        int $longPoll = 0,
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

        $headers = [
            'Accept' => 'application/vnd.eventstore.atom+json',
        ];

        if (! $resolveLinkTos) {
            $headers['ES-ResolveLinkTos'] = 'false';
        }

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        if ($longPoll > 0) {
            $headers['ES-LongPoll'] = $longPoll;
        }

        $response = $this->httpClient->get(
            '/streams/%24all' . '/' . $position->asString() . '/forward/' . $count . '?embed=tryharder',
            $headers,
            $userCredentials,
            $this->onException
        );

        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toStream('$all');
            case 200:
                $json = Json::decode($response->getBody()->getContents());

                foreach ($json['links'] as $link) {
                    if ($link['relation'] === 'next') {
                        $start = \strlen($this->baseUri . '/streams/%24all' . '/');
                        $nextPosition = Position::parse(\substr($link['uri'], $start, 32));
                    }
                }

                $events = [];
                foreach (\array_reverse($json['entries']) as $entry) {
                    $data = $entry['data'] ?? '';

                    if (\is_array($data)) {
                        $data = Json::encode($data);
                    }

                    $metadata = $json['metadata'] ?? '';

                    if (\is_array($metadata)) {
                        $metadata = Json::encode($metadata);
                    }

                    $event = new RecordedEvent(
                        $json['streamId'],
                        $json['positionEventNumber'],
                        EventId::fromString($json['eventId']),
                        $json['eventType'],
                        $json['isJson'],
                        $data,
                        $metadata,
                        DateTime::create($json['updated'])
                    );

                    $events[] = new ResolvedEvent($event, null, null);
                }

                return new AllEventsSlice(
                    ReadDirection::forward(),
                    $position,
                    $nextPosition,
                    $events
                );
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
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

        $headers = [
            'Accept' => 'application/vnd.eventstore.atom+json',
        ];

        if (! $resolveLinkTos) {
            $headers['ES-ResolveLinkTos'] = 'false';
        }

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $response = $this->httpClient->get(
            '/streams/%24all' . '/' . $position->asString() . '/backward/' . $count . '?embed=tryharder',
            $headers,
            $userCredentials,
            $this->onException
        );

        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toStream('$all');
            case 200:
                $json = Json::decode($response->getBody()->getContents());

                foreach ($json['links'] as $link) {
                    if ($link['relation'] === 'previous') {
                        $start = \strlen($this->baseUri . '/streams/%24all' . '/');
                        $nextPosition = Position::parse(\substr($link['uri'], $start, 32));
                    }
                }

                $events = [];
                foreach ($json['entries'] as $entry) {
                    $data = $entry['data'] ?? '';

                    if (\is_array($data)) {
                        $data = Json::encode($data);
                    }

                    $metadata = $json['metadata'] ?? '';

                    if (\is_array($metadata)) {
                        $metadata = Json::encode($metadata);
                    }

                    $event = new RecordedEvent(
                        $json['streamId'],
                        $json['positionEventNumber'],
                        EventId::fromString($json['eventId']),
                        $json['eventType'],
                        $json['isJson'],
                        $data,
                        $metadata,
                        DateTime::create($json['updated'])
                    );

                    $events[] = new ResolvedEvent($event, null, null);
                }

                return new AllEventsSlice(
                    ReadDirection::backward(),
                    $position,
                    $nextPosition,
                    $events
                );
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
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

        return $this->appendToStream(
            $stream,
            $expectedMetaStreamVersion,
            [$metaEvent],
            $userCredentials
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

        $body = Json::encode($settings);

        $headers = [
            'Content-Type' => 'application/json',
            'Content-Length' => \strlen($body),
        ];

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $response = $this->httpClient->put(
            '/subscriptions/' . \urlencode($stream) . '/' . \urlencode($groupName),
            $headers,
            $body,
            $userCredentials,
            $this->onException
        );

        $json = Json::decode($response->getBody()->getContents());

        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 201:
            case 409:
                return new PersistentSubscriptionCreateResult(
                    PersistentSubscriptionCreateStatus::byName($json['result'])
                );
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
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

        $body = Json::encode($settings);

        $headers = [
            'Content-Type' => 'application/json',
            'Content-Length' => \strlen($body),
        ];

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $response = $this->httpClient->post(
            '/subscriptions/' . \urlencode($stream) . '/' . \urlencode($groupName),
            $headers,
            $body,
            $userCredentials,
            $this->onException
        );

        switch ($response->getStatusCode()) {
            case 200:
                return new PersistentSubscriptionUpdateResult(
                    PersistentSubscriptionUpdateStatus::success()
                );
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 404:
                return new PersistentSubscriptionUpdateResult(
                    PersistentSubscriptionUpdateStatus::notFound()
                );
            default:
                throw new EventStoreConnectionException(\sprintf(
                    'Unexpected status code %d returned',
                    $response->getStatusCode()
                ));
        }
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

        $headers = [];

        if ($this->settings->requireMaster()) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $response = $this->httpClient->delete(
            '/subscriptions/' . \urlencode($stream) . '/' . \urlencode($groupName),
            $headers,
            $userCredentials,
            $this->onException
        );

        $json = Json::decode($response->getBody()->getContents());

        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 200:
            case 404:
                return new PersistentSubscriptionDeleteResult(
                    PersistentSubscriptionDeleteStatus::byName($json['result'])
                );
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
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
