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

namespace Prooph\EventStoreHttpClient\ClientOperations;

use Http\Message\RequestFactory;
use Http\Message\UriFactory;
use Prooph\EventStoreHttpClient\Common\SystemEventTypes;
use Prooph\EventStoreHttpClient\EventId;
use Prooph\EventStoreHttpClient\EventReadResult;
use Prooph\EventStoreHttpClient\EventReadStatus;
use Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Prooph\EventStoreHttpClient\Http\HttpMethod;
use Prooph\EventStoreHttpClient\RecordedEvent;
use Prooph\EventStoreHttpClient\ResolvedEvent;
use Prooph\EventStoreHttpClient\UserCredentials;
use Prooph\EventStoreHttpClient\Util\DateTime;
use Prooph\EventStoreHttpClient\Util\Json;

/** @internal */
class ReadEventOperation extends Operation
{
    public function __invoke(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        int $eventNumber,
        bool $resolveLinkTos,
        ?UserCredentials $userCredentials,
        bool $requireMaster
    ): EventReadResult {
        $headers = [
            'Accept' => 'application/vnd.eventstore.atom+json',
        ];

        if ($requireMaster) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        if (-1 === $eventNumber) {
            $eventNumber = 'head';
        }

        $request = $requestFactory->createRequest(
            HttpMethod::GET,
            $uriFactory->createUri(
                $baseUri . '/streams/' . \urlencode($stream) . '/' . $eventNumber . '?embed=tryharder'
            ),
            $headers
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

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
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
