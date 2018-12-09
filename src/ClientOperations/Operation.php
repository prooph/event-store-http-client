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

use Http\Client\Exception as HttpClientException;
use Http\Message\Authentication\BasicAuth;
use Prooph\EventStoreHttpClient\Exception\EventStoreConnectionException;
use Prooph\EventStoreHttpClient\UserCredentials;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

/** @internal */
abstract class Operation
{
    protected function sendRequest(
        ClientInterface $httpClient,
        ?UserCredentials $userCredentials,
        RequestInterface $request
    ): ResponseInterface {
        if ($userCredentials) {
            $auth = new BasicAuth($userCredentials->username(), $userCredentials->password());
            $request = $auth->authenticate($request);
        }

        try {
            return $httpClient->sendRequest($request);
        } catch (HttpClientException $e) {
            throw new EventStoreConnectionException($e->getMessage());
        } catch (Throwable $e) {
            throw new EventStoreConnectionException($e->getMessage());
        }
    }
}
