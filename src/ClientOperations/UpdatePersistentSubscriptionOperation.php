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
use Prooph\EventStore\Internal\PersistentSubscriptionUpdateResult;
use Prooph\EventStore\Internal\PersistentSubscriptionUpdateStatus;
use Prooph\EventStore\PersistentSubscriptionSettings;
use Prooph\EventStore\Transport\Http\HttpMethod;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStore\Util\Json;
use Prooph\EventStoreHttpClient\Http\HttpClient;

/** @internal  */
class UpdatePersistentSubscriptionOperation extends Operation
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
    ): PersistentSubscriptionUpdateResult {
        $body = Json::encode($settings);

        $headers = [
            'Content-Type' => 'application/json',
            'Content-Length' => \strlen($body),
        ];

        if ($requireMaster) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $request = $requestFactory->createRequest(
            HttpMethod::POST,
            $uriFactory->createUri($baseUri . '/subscriptions/' . \urlencode($stream) . '/' . \urlencode($groupName)),
            $headers,
            $body
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 200:
                return new PersistentSubscriptionUpdateResult(
                    PersistentSubscriptionUpdateStatus::success()
                );
            case 401:
                throw AccessDeniedException::toSubscription($stream, $groupName);
            case 404:
                return new PersistentSubscriptionUpdateResult(
                    PersistentSubscriptionUpdateStatus::notFound()
                );
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
