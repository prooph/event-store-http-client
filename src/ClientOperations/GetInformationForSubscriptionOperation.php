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
use Prooph\EventStore\Data\DetailedSubscriptionInformation;
use Prooph\EventStore\Data\PersistentSubscriptionSettings;
use Prooph\EventStore\NamedConsumerStrategy;

Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStoreHttpClient\Http\HttpMethod;
use Prooph\EventStoreHttpClient\UserCredentials;
use Psr\Http\Client\ClientInterface;

/** @internal */
class GetInformationForSubscriptionOperation extends Operation
{
    public function __invoke(
        ClientInterface $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        string $groupName,
        ?UserCredentials $userCredentials
    ): DetailedSubscriptionInformation {
        $request = $requestFactory->createRequest(
            HttpMethod::GET,
            $uriFactory->createUri($baseUri . '/subscriptions/' . \urlencode($stream) . '/' . \urlencode($groupName) . '/info')
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 200:
                $json = \json_decode($response->getBody()->getContents(), true);

                return new DetailedSubscriptionInformation(
                    new PersistentSubscriptionSettings(
                        $json['config']['resolveLinktos'],
                        $json['config']['startFrom'],
                        $json['config']['extraStatistics'],
                        $json['config']['checkPointAfterMilliseconds'],
                        $json['config']['liveBufferSize'],
                        $json['config']['readBatchSize'],
                        $json['config']['bufferSize'],
                        $json['config']['maxCheckPointCount'],
                        $json['config']['maxRetryCount'],
                        $json['config']['maxSubscriberCount'],
                        $json['config']['messageTimeoutMilliseconds'],
                        $json['config']['minCheckPointCount'],
                        NamedConsumerStrategy::byName($json['config']['namedConsumerStrategy'])
                    ),
                    $json['eventStreamId'],
                    $json['groupName'],
                    $json['status'],
                    $json['averageItemsPerSecond'],
                    $json['totalItemsProcessed'],
                    $json['countSinceLastMeasurement'],
                    $json['lastProcessedEventNumber'],
                    $json['lastKnownEventNumber'],
                    $json['readBufferCount'],
                    $json['liveBufferCount'],
                    $json['retryBufferCount'],
                    $json['totalInFlightMessages']
                );
            case 401:
                throw new AccessDenied();
            case 404:
                throw new \RuntimeException(\sprintf(
                    'Subscription with stream \'%s\' and group name \'%s\' not found',
                    $stream,
                    $groupName
                ));
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
