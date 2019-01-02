<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2019 prooph software GmbH <contact@prooph.de>
 * (c) 2018-2019 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStoreHttpClient\Http;

use Http\Message\RequestFactory;
use Prooph\EventStore\Transport\Http\HttpMethod;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\ConnectionSettings;
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
    /** @var ConnectionSettings */
    private $settings;
    /** @var string */
    private $baseUri;

    public function __construct(
        ClientInterface $client,
        RequestFactory $requestFactory,
        ConnectionSettings $settings,
        string $baseUri
    ) {
        $this->client = $client;
        $this->requestFactory = $requestFactory;
        $this->settings = $settings;
        $this->baseUri = $baseUri;
    }

    public function get(
        string $uri,
        array $headers,
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        return $this->receive(
            HttpMethod::GET,
            $this->baseUri . $uri,
            $headers,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $onException
        );
    }

    public function post(
        string $uri,
        array $headers,
        string $body,
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        return $this->send(
            HttpMethod::POST,
            $this->baseUri . $uri,
            $headers,
            $body,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
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
            $this->baseUri . $uri,
            $headers,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
            $onException
        );
    }

    public function put(
        string $url,
        array $headers,
        string $body,
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        return $this->send(
            HttpMethod::PUT,
            $this->baseUri . $url,
            $headers,
            $body,
            $userCredentials ?? $this->settings->defaultUserCredentials(),
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
        ?UserCredentials $userCredentials,
        callable $onException
    ): ResponseInterface {
        $request = $this->requestFactory->createRequest($method, $uri, $headers, $body);

        if (null !== $userCredentials) {
            $request = $this->addAuthenticationHeader($request, $userCredentials);
        }

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
