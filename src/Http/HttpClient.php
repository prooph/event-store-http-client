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

namespace Prooph\EventStoreHttpClient\Http;

use Http\Message\RequestFactory;
use Http\Message\ResponseFactory;
use Prooph\EventStoreHttpClient\UserCredentials;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

/** @internal  */
class HttpClient
{
    /** @var ClientInterface */
    private $client;
    /** @var RequestFactory */
    private $requestFactory;
    /** @var ResponseFactory */
    private $responseFactory;

    public function __construct(
        ClientInterface $client,
        RequestFactory $requestFactory,
        ResponseFactory $responseFactory
    ) {
        $this->client = $client;
        $this->requestFactory = $requestFactory;
        $this->responseFactory = $responseFactory;
    }

    public function get(
        string $uri,
        array $headers,
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        return $this->receive(
            HttpMethod::GET,
            $uri,
            $headers,
            $userCredentials,
            $onException
        );
    }

    public function post(
        string $uri,
        array $headers,
        string $body,
        string $contentType,
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        return $this->send(
            HttpMethod::POST,
            $uri,
            $headers,
            $body,
            $contentType,
            $userCredentials,
            $onException
        );
    }

    public function delete(
        string $uri,
        array $headers,
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        return $this->receive(
            HttpMethod::DELETE,
            $uri,
            $headers,
            $userCredentials,
            $onException
        );
    }

    public function put(
        string $url,
        array $headers,
        string $body,
        string $contentType,
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        return $this->send(
            HttpMethod::PUT,
            $url,
            $headers,
            $body,
            $contentType,
            $userCredentials,
            $onException
        );
    }

    private function receive(
        string $method,
        string $uri,
        array $headers,
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        $request = $this->requestFactory->createRequest($method, $uri, $headers);

        if (null !== $userCredentials) {
            $request = $this->addAuthenticationHeader($request, $userCredentials);
        }

        try {
            return $this->client->sendRequest($request);
        } catch (Throwable $e) {
            $onException($e);
        }
    }

    private function send(
        string $method,
        string $uri,
        array $headers,
        string $body,
        string $contentType,
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        $request = $this->requestFactory->createRequest($method, $uri, $headers, $body);

        if (null !== $userCredentials) {
            $request = $this->addAuthenticationHeader($request, $userCredentials);
        }

        $request = $request->withHeader('Content-Type', $contentType);
        $request = $request->withHeader('Content-Length', (string) \strlen($body));

        try {
            return $this->client->sendRequest($request);
        } catch (Throwable $e) {
            $onException($e);
        }
    }

    private function addAuthenticationHeader(
        RequestInterface $request,
        UserCredentials $userCredentials
    ): RequestInterface {
        $httpAuthentication = \sprintf(
            '%s:%s',
            $userCredentials->username(),
            $userCredentials->password()
        );

        $encodedCredentials = \base64_encode($httpAuthentication);

        return $request->withHeader('Authorization', 'Basic ' . $encodedCredentials);
    }
}
