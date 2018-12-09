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
use Prooph\EventStore\Internal\Data\PersistentSubscriptionDeleteResult;

Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStore\Internal\Data\PersistentSubscriptionDeleteStatus;
use Prooph\EventStoreHttpClient\Http\Method;
use Prooph\EventStoreHttpClient\UserCredentials;

/** @internal  */
class DeletePersistentSubscriptionOperation extends Operation
{
    public function __invoke(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        string $groupName,
        ?UserCredentials $userCredentials
    ): PersistentSubscriptionDeleteResult {
        $request = $requestFactory->createRequest(
            Method::Delete,
            $uriFactory->createUri($baseUri . '/subscriptions/' . \urlencode($stream) . '/' . \urlencode($groupName))
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        $json = \json_decode($response->getBody()->getContents(), true);
        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toSubscription($stream, $groupName);
            case 200:
            case 404:
                return new PersistentSubscriptionDeleteResult(
                    PersistentSubscriptionDeleteStatus::byName($json['result'])
                );
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
