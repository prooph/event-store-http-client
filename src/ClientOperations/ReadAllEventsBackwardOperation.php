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
use Prooph\EventStoreHttpClient\AllEventsSlice;
use Prooph\EventStoreHttpClient\EventId;
use Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Prooph\EventStoreHttpClient\Http\HttpMethod;
use Prooph\EventStoreHttpClient\Position;
use Prooph\EventStoreHttpClient\ReadDirection;
use Prooph\EventStoreHttpClient\RecordedEvent;
use Prooph\EventStoreHttpClient\ResolvedEvent;
use Prooph\EventStoreHttpClient\UserCredentials;
use Prooph\EventStoreHttpClient\Util\DateTime;
use Prooph\EventStoreHttpClient\Util\Json;

/** @internal */
class ReadAllEventsBackwardOperation extends Operation
{
    public function __invoke(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        Position $position,
        int $count,
        bool $resolveLinkTos,
        int $longPoll,
        ?UserCredentials $userCredentials,
        bool $requireMaster
    ): AllEventsSlice {
        $headers = [
            'Accept' => 'application/vnd.eventstore.atom+json',
        ];

        if (! $resolveLinkTos) {
            $headers['ES-ResolveLinkTos'] = 'false';
        }

        if ($requireMaster) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        if ($longPoll > 0) {
            $headers['ES-LongPoll'] = $longPoll;
        }

        $request = $requestFactory->createRequest(
            HttpMethod::GET,
            $uriFactory->createUri(
                $baseUri . '/streams/%24all' . '/' . $position->asString() . '/backward/' . $count . '?embed=tryharder'
            ),
            $headers
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toStream('$all');
            case 200:
                $json = Json::decode($response->getBody()->getContents());

                foreach ($json['links'] as $link) {
                    if ($link['relation'] === 'previous') {
                        $start = \strlen($baseUri . '/streams/%24all' . '/');
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
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
