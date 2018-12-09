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

namespace Prooph\EventStoreHttpClient\UserManagement;

use Prooph\EventStoreHttpClient\EndPoint;
use Prooph\EventStoreHttpClient\Exception\EventStoreConnectionException;
use Prooph\EventStoreHttpClient\Exception\UserCommandConflictException;
use Prooph\EventStoreHttpClient\Exception\UserCommandFailedException;
use Prooph\EventStoreHttpClient\Http\EndpointExtensions;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Prooph\EventStoreHttpClient\Http\HttpStatusCode;
use Prooph\EventStoreHttpClient\UserCredentials;
use Prooph\EventStoreHttpClient\Util\Json;
use Psr\Http\Message\ResponseInterface;
use Throwable;

/** @internal */
class UsersClient
{
    /** @var HttpClient */
    private $client;

    public function __construct(HttpClient $client)
    {
        $this->client = $client;
    }

    public function enable(
        EndPoint $endPoint,
        string $login,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): void {
        $this->sendPost(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                '/users/%s/command/enable',
                $login
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    public function disable(
        EndPoint $endPoint,
        string $login,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): void {
        $this->sendPost(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                '/users/%s/command/disable',
                $login
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    public function delete(
        EndPoint $endPoint,
        string $login,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): void {
        $this->sendDelete(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                '/users/%s',
                $login
            ),
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    /** @return UserDetails[] */
    public function listAll(
        EndPoint $endPoint,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): array {
        $response = $this->sendGet(
            EndpointExtensions::rawUrlToHttpUrl($endPoint, $httpSchema, '/users/'),
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($response->getBody()->getContents());

        $userDetails = [];

        foreach ($data['data'] as $entry) {
            $userDetails[] = UserDetails::fromArray($entry);
        }

        return $userDetails;
    }

    public function getCurrentUser(
        EndPoint $endPoint,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): UserDetails {
        $response = $this->sendGet(
            EndpointExtensions::rawUrlToHttpUrl(
                $endPoint,
                $httpSchema,
                '/users/$current'
            ),
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($response->getBody()->getContents());

        return UserDetails::fromArray($data['data']);
    }

    public function getUser(
        EndPoint $endPoint,
        string $login,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): UserDetails {
        $response = $this->sendGet(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                '/users/%s',
                $login
            ),
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($response->getBody()->getContents());

        return UserDetails::fromArray($data['data']);
    }

    public function createUser(
        EndPoint $endPoint,
        UserCreationInformation $newUser,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): void {
        $this->sendPost(
            EndpointExtensions::rawUrlToHttpUrl(
                $endPoint,
                $httpSchema,
                '/users/'
            ),
            Json::encode($newUser),
            $userCredentials,
            HttpStatusCode::CREATED
        );
    }

    public function updateUser(
        EndPoint $endPoint,
        string $login,
        UserUpdateInformation $updatedUser,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): void {
        $this->sendPut(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                '/users/%s',
                $login
            ),
            Json::encode($updatedUser),
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    public function changePassword(
        EndPoint $endPoint,
        string $login,
        ChangePasswordDetails $changePasswordDetails,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): void {
        $this->sendPost(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                '/users/%s/command/change-password',
                $login
            ),
            Json::encode($changePasswordDetails),
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    public function resetPassword(
        EndPoint $endPoint,
        string $login,
        ResetPasswordDetails $resetPasswordDetails,
        ?UserCredentials $userCredentials = null,
        string $httpSchema = EndpointExtensions::HTTP_SCHEMA
    ): void {
        $this->sendPost(
            EndpointExtensions::formatStringToHttpUrl(
                $endPoint,
                $httpSchema,
                '/users/%s/command/reset-password',
                $login
            ),
            Json::encode($resetPasswordDetails),
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    private function sendGet(
        string $uri,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): ResponseInterface {
        $response = $this->client->get($uri, [], $userCredentials, static function (Throwable $e) {
            throw new EventStoreConnectionException($e->getMessage());
        });

        if ($response->getStatusCode() !== $expectedCode) {
            throw new UserCommandFailedException(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for GET on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $uri
                )
            );
        }
    }

    private function sendDelete(
        string $uri,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): void {
        $response = $this->client->delete($uri, [], $userCredentials, static function (Throwable $e) {
            throw new EventStoreConnectionException($e->getMessage());
        });

        if ($response->getStatusCode() !== $expectedCode) {
            throw new UserCommandFailedException(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for DELETE on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $uri
                )
            );
        }
    }

    private function sendPut(
        string $uri,
        string $content,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): void {
        $response = $this->client->put(
            $uri,
            [],
            $content,
            'application/json',
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() !== $expectedCode) {
            throw new UserCommandFailedException(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for PUT on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $uri
                )
            );
        }
    }

    private function sendPost(
        string $uri,
        string $content,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): void {
        $response = $this->client->put(
            $uri,
            [],
            $content,
            'application/json',
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() === HttpStatusCode::CONFLICT) {
            throw new UserCommandConflictException($response->getStatusCode(), $response->getReasonPhrase());
        }

        if ($response->getStatusCode() !== $expectedCode) {
            throw new UserCommandFailedException(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for POST on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $uri
                )
            );
        }
    }
}
