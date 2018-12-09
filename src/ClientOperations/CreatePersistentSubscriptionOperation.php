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
use Prooph\EventStore\Data\PersistentSubscriptionSettings;
use Prooph\EventStore\Internal\Data\PersistentSubscriptionCreateResult;

Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStore\Internal\Data\PersistentSubscriptionCreateStatus;
use Prooph\EventStoreHttpClient\Http\Method;
use Prooph\EventStoreHttpClient\UserCredentials;

/** @internal  */
class CreatePersistentSubscriptionOperation extends Operation
{
    public function __invoke(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        string $groupName,
        PersistentSubscriptionSettings $settings,
        ?UserCredentials $userCredentials
    ): PersistentSubscriptionCreateResult {
        $string = \json_encode($settings->toArray());

        $request = $requestFactory->createRequest(
            Method::Put,
            $uriFactory->createUri($baseUri . '/subscriptions/' . \urlencode($stream) . '/' . \urlencode($groupName)),
            [
                'Content-Type' => 'application/json',
                'Content-Length' => \strlen($string),
            ],
            $string
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        $json = \json_decode($response->getBody()->getContents(), true);
        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toSubscription($stream, $groupName);
            case 201:
            case 409:
                return new PersistentSubscriptionCreateResult(
                    PersistentSubscriptionCreateStatus::byName($json['result'])
                );
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
