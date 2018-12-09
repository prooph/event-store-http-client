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
use Prooph\EventStore\SubscriptionInformation;

Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStoreHttpClient\Http\Method;
use Prooph\EventStoreHttpClient\UserCredentials;

/** @internal */
class GetInformationForSubscriptionsWithStreamOperation extends Operation
{
    /**
     * @return SubscriptionInformation[]
     */
    public function __invoke(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        ?UserCredentials $userCredentials
    ): array {
        $request = $requestFactory->createRequest(
            Method::Get,
            $uriFactory->createUri($baseUri . '/subscriptions/' . \urlencode($stream))
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 401:
                throw new AccessDenied();
            case 200:
                $json = \json_decode($response->getBody()->getContents(), true);

                $result = [];

                foreach ($json as $entry) {
                    $result[] = new SubscriptionInformation(
                        $entry['eventStreamId'],
                        $entry['groupName'],
                        $entry['status'],
                        $entry['averageItemsPerSecond'],
                        $entry['totalItemsProcessed'],
                        $entry['lastProcessedEventNumber'],
                        $entry['lastKnownEventNumber'],
                        $entry['connectionCount'],
                        $entry['totalInFlightMessages']
                    );
                }

                return $result;
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
