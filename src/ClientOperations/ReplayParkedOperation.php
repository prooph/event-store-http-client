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
use Prooph\EventStore\Internal\Data\ReplayParkedResult;
use Prooph\EventStore\Internal\Data\ReplayParkedStatus;
use Prooph\EventStoreHttpClient\Exception\AccessDeniedException;
use Prooph\EventStoreHttpClient\Http\HttpMethod;
use Prooph\EventStoreHttpClient\UserCredentials;

/** @internal */
class ReplayParkedOperation extends Operation
{
    public function __invoke(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        string $groupName,
        ?UserCredentials $userCredentials
    ): ReplayParkedResult {
        $request = $requestFactory->createRequest(
            HttpMethod::POST,
            $uriFactory->createUri(\sprintf(
                '%s/subscriptions/%s/%s/replayParked',
                $baseUri,
                \urlencode($stream),
                \urlencode($groupName)
            )),
            [
                'Content-Length' => 0,
            ],
            ''
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 401:
                throw AccessDeniedException::toStream($stream);
            case 404:
            case 200:
                $json = \json_decode($response->getBody()->getContents(), true);

                return new ReplayParkedResult(
                    $json['correlationId'],
                    $json['reason'],
                    ReplayParkedStatus::byName($json['result'])
                );
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
