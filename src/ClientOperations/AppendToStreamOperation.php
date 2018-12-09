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

use Http\Client\HttpClient;
use Http\Message\RequestFactory;
use Http\Message\UriFactory;
use Prooph\EventStoreHttpClient\EventData;
use Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStoreHttpClient\Exception\RuntimeException;
use Prooph\EventStoreHttpClient\Exception\StreamDeletedException;
use Prooph\EventStoreHttpClient\Exception\WrongExpectedVersionException;
use Prooph\EventStoreHttpClient\Http\Method;
use Prooph\EventStoreHttpClient\UserCredentials;
use Prooph\EventStoreHttpClient\Util\Json;

/** @internal */
class AppendToStreamOperation extends Operation
{
    public function __invoke(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        int $expectedVersion,
        array $events,
        ?UserCredentials $userCredentials,
        bool $requireMaster
    ): void {
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

        if ($requireMaster) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $request = $requestFactory->createRequest(
            Method::Post,
            $uriFactory->createUri($baseUri . '/streams/' . \urlencode($stream)),
            $headers,
            $body
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 201:
                return;
            case 400:
                $header = $response->getHeader('ES-CurrentVersion');

                if (empty($header)) {
                    throw new RuntimeException($response->getReasonPhrase());
                }

                $currentVersion = (int) $header[0];

                throw WrongExpectedVersionException::with($stream, $expectedVersion, $currentVersion);
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 410:
                throw StreamDeletedException::with($stream);
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
