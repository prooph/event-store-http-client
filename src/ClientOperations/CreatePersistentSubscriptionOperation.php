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
use Prooph\EventStore\Exception\AccessDeniedException;
use Prooph\EventStore\Internal\PersistentSubscriptionCreateResult;
use Prooph\EventStore\Internal\PersistentSubscriptionCreateStatus;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\Transport\Http\HttpMethod;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStore\Util\Json;
use Prooph\EventStoreHttpClient\Http\HttpClient;

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
        ?UserCredentials $userCredentials,
        bool $requireMaster
    ): PersistentSubscriptionCreateResult {
        $body = Json::encode($settings);

        $headers = [
            'Content-Type' => 'application/json',
            'Content-Length' => \strlen($body),
        ];

        if ($requireMaster) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $request = $requestFactory->createRequest(
            HttpMethod::PUT,
            $uriFactory->createUri($baseUri . '/subscriptions/' . \urlencode($stream) . '/' . \urlencode($groupName)),
            $headers,
            $body
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        $json = Json::decode($response->getBody()->getContents());

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
