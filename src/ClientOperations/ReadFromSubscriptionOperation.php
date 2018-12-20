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
use Prooph\EventStoreHttpClient\EventId;
use Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Prooph\EventStoreHttpClient\Http\HttpMethod;
use Prooph\EventStoreHttpClient\RecordedEvent;
use Prooph\EventStoreHttpClient\UserCredentials;
use Prooph\EventStoreHttpClient\Util\DateTime;

/** @internal */
class ReadFromSubscriptionOperation extends Operation
{
    /**
     * @return RecordedEvent[]
     */
    public function __invoke(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        string $groupName,
        int $amount,
        ?UserCredentials $userCredentials
    ): array {
        $request = $requestFactory->createRequest(
            HttpMethod::GET,
            $uriFactory->createUri(\sprintf(
                '%s/subscriptions/%s/%s/%d?embed=tryharder',
                $baseUri,
                \urlencode($stream),
                \urlencode($groupName),
                $amount
            )),
            [
                'Accept' => 'application/vnd.eventstore.competingatom+json',
            ]
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 404:
                throw new \RuntimeException(\sprintf(
                    'Subscription with stream \'%s\' and group name \'%s\' not found',
                    $stream,
                    $groupName
                ));
            case 200:
                $json = \json_decode($response->getBody()->getContents(), true);
                $events = [];

                if (null === $json) {
                    return $events;
                }

                foreach (\array_reverse($json['entries']) as $entry) {
                    $data = $entry['data'] ?? '';

                    if (\is_array($data)) {
                        $data = \json_encode($data);
                    }

                    $field = isset($json['isLinkMetaData']) && $json['isLinkMetaData'] ? 'linkMetaData' : 'metaData';

                    $metadata = $json[$field] ?? '';

                    if (\is_array($metadata)) {
                        $metadata = \json_encode($metadata);
                    }

                    $events[] = new RecordedEvent(
                        $entry['positionStreamId'],
                        $entry['positionEventNumber'],
                        EventId::fromString($entry['eventId']),
                        $entry['eventType'],
                        $entry['isJson'],
                        $data,
                        $metadata,
                        DateTime::create($entry['updated'])
                    );
                }

                return $events;
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
