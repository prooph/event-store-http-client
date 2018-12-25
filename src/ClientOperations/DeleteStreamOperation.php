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
use Prooph\EventStore\DeleteResult;
use Prooph\EventStore\Exception\AccessDeniedException;
use Prooph\EventStore\Position;
use Prooph\EventStore\Transport\Http\HttpMethod;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\Http\HttpClient;

/** @internal */
class DeleteStreamOperation extends Operation
{
    public function __invoke(
        HttpClient $httpClient,
        RequestFactory $requestFactory,
        UriFactory $uriFactory,
        string $baseUri,
        string $stream,
        int $expectedVersion,
        bool $hardDelete,
        ?UserCredentials $userCredentials,
        bool $requireMaster
    ): DeleteResult {
        $headers = [
            'ES-ExpectedVersion' => $expectedVersion,
        ];

        if ($hardDelete) {
            $headers['ES-HardDelete'] = 'true';
        }

        if ($requireMaster) {
            $headers['ES-RequiresMaster'] = 'true';
        }

        $request = $requestFactory->createRequest(
            HttpMethod::DELETE,
            $uriFactory->createUri($baseUri . '/streams/' . \urlencode($stream)),
            $headers
        );

        $response = $this->sendRequest($httpClient, $userCredentials, $request);

        switch ($response->getStatusCode()) {
            case 204:
            case 410:
                return new DeleteResult(Position::invalid());
            case 401:
                throw AccessDeniedException::toStream($stream);
            default:
                throw new \UnexpectedValueException('Unexpected status code ' . $response->getStatusCode() . ' returned');
        }
    }
}
