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

namespace Prooph\EventStoreHttpClient\UserManagement;

use Http\Message\RequestFactory;
use Prooph\EventStore\Exception\EventStoreConnectionException;
use Prooph\EventStore\Exception\InvalidArgumentException;
use Prooph\EventStore\Transport\Http\HttpStatusCode;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStore\UserManagement\ChangePasswordDetails;
use Prooph\EventStore\UserManagement\ResetPasswordDetails;
use Prooph\EventStore\UserManagement\UserCreationInformation;
use Prooph\EventStore\UserManagement\UserDetails;
use Prooph\EventStore\UserManagement\UsersManager as SyncUsersManager;
use Prooph\EventStore\UserManagement\UserUpdateInformation;
use Prooph\EventStore\Util\Json;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Prooph\EventStoreHttpClient\Exception\UserCommandConflictException;
use Prooph\EventStoreHttpClient\Exception\UserCommandFailed;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\ResponseInterface;
use Throwable;

class UsersManager implements SyncUsersManager
{
    /** @var ConnectionSettings */
    private $settings;
    /** @var HttpClient */
    private $httpClient;

    /** @internal */
    public function __construct(
        ConnectionSettings $settings,
        ClientInterface $client,
        RequestFactory $requestFactory
    ) {
        $this->settings = $settings;

        $this->httpClient = new HttpClient(
            $client,
            $requestFactory,
            $settings,
            \sprintf(
                '%s://%s:%s',
                $settings->schema(),
                $settings->endPoint()->host(),
                $settings->endPoint()->port()
            )
        );
    }

    public function enable(string $login, ?UserCredentials $userCredentials = null): void
    {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        $this->sendPost(
            \sprintf(
                '/users/%s/command/enable',
                \urlencode($login)
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    public function disable(string $login, ?UserCredentials $userCredentials = null): void
    {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        $this->sendPost(
            \sprintf(
                '/users/%s/command/disable',
                \urlencode($login)
            ),
            '',
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    /** @throws UserCommandFailed */
    public function deleteUser(string $login, ?UserCredentials $userCredentials = null): void
    {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        $this->sendDelete(
            \sprintf(
                '/users/%s',
                \urlencode($login)
            ),
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    /** @return UserDetails[] */
    public function listAll(?UserCredentials $userCredentials = null): array
    {
        $response = $this->sendGet(
            '/users/',
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($response->getBody()->getContents());

        $userDetails = [];

        foreach ($data['data'] as $entry) {
            if (isset($entry['dateLastUpdated'])) {
                $entry['dateLastUpdated'] = $this->createDateTimeString($entry['dateLastUpdated']);
            }

            $userDetails[] = UserDetails::fromArray($entry);
        }

        return $userDetails;
    }

    public function getCurrentUser(?UserCredentials $userCredentials = null): UserDetails
    {
        $response = $this->sendGet(
            '/users/$current',
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($response->getBody()->getContents());

        if (isset($data['data']['dateLastUpdated'])) {
            $data['data']['dateLastUpdated'] = $this->createDateTimeString($data['data']['dateLastUpdated']);
        }

        return UserDetails::fromArray($data['data']);
    }

    public function getUser(string $login, ?UserCredentials $userCredentials = null): UserDetails
    {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        $response = $this->sendGet(
            \sprintf(
                '/users/%s',
                \urlencode($login)
            ),
            $userCredentials,
            HttpStatusCode::OK
        );

        $data = Json::decode($response->getBody()->getContents());

        if (isset($data['data']['dateLastUpdated'])) {
            $data['data']['dateLastUpdated'] = $this->createDateTimeString($data['data']['dateLastUpdated']);
        }

        return UserDetails::fromArray($data['data']);
    }

    /**
     * @param string $login
     * @param string $fullName
     * @param string[] $groups
     * @param string $password
     * @param UserCredentials|null $userCredentials
     * @return void
     */
    public function createUser(
        string $login,
        string $fullName,
        array $groups,
        string $password,
        ?UserCredentials $userCredentials = null
    ): void {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        if (empty($fullName)) {
            throw new InvalidArgumentException('FullName cannot be empty');
        }

        if (empty($password)) {
            throw new InvalidArgumentException('Password cannot be empty');
        }

        foreach ($groups as $group) {
            if (! \is_string($group) || empty($group)) {
                throw new InvalidArgumentException('Expected an array of non empty strings for group');
            }
        }

        $this->sendPost(
            '/users/',
            Json::encode(new UserCreationInformation(
                $login,
                $fullName,
                $groups,
                $password
            )),
            $userCredentials,
            HttpStatusCode::CREATED
        );
    }

    /**
     * @param string $login
     * @param string $fullName
     * @param string[] $groups
     * @param UserCredentials|null $userCredentials
     * @return void
     */
    public function updateUser(
        string $login,
        string $fullName,
        array $groups,
        ?UserCredentials $userCredentials = null
    ): void {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        if (empty($fullName)) {
            throw new InvalidArgumentException('FullName cannot be empty');
        }

        foreach ($groups as $group) {
            if (! \is_string($group) || empty($group)) {
                throw new InvalidArgumentException('Expected an array of non empty strings for group');
            }
        }

        $this->sendPut(
            \sprintf(
                '/users/%s',
                \urlencode($login)
            ),
            Json::encode(new UserUpdateInformation($fullName, $groups)),
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    public function changePassword(
        string $login,
        string $oldPassword,
        string $newPassword,
        ?UserCredentials $userCredentials = null
    ): void {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        if (empty($oldPassword)) {
            throw new InvalidArgumentException('Old password cannot be empty');
        }

        if (empty($newPassword)) {
            throw new InvalidArgumentException('New password cannot be empty');
        }

        $this->sendPost(
            \sprintf(
                '/users/%s/command/change-password',
                \urlencode($login)
            ),
            Json::encode(new ChangePasswordDetails($oldPassword, $newPassword)),
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    public function resetPassword(
        string $login,
        string $newPassword,
        ?UserCredentials $userCredentials = null
    ): void {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        if (empty($newPassword)) {
            throw new InvalidArgumentException('New password cannot be empty');
        }

        $this->sendPost(
            \sprintf(
                '/users/%s/command/reset-password',
                \urlencode($login)
            ),
            Json::encode(new ResetPasswordDetails($newPassword)),
            $userCredentials,
            HttpStatusCode::OK
        );
    }

    private function sendGet(
        string $uri,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): ResponseInterface {
        $response = $this->httpClient->get($uri, [], $userCredentials, static function (Throwable $e) {
            throw new EventStoreConnectionException($e->getMessage());
        });

        if ($response->getStatusCode() !== $expectedCode) {
            throw new UserCommandFailed(
                $response->getStatusCode(),
                \sprintf(
                    'Server returned %d (%s) for GET on %s',
                    $response->getStatusCode(),
                    $response->getReasonPhrase(),
                    $uri
                )
            );
        }

        return $response;
    }

    private function sendDelete(
        string $uri,
        ?UserCredentials $userCredentials,
        int $expectedCode
    ): void {
        $response = $this->httpClient->delete($uri, [], $userCredentials, static function (Throwable $e) {
            throw new EventStoreConnectionException($e->getMessage());
        });

        if ($response->getStatusCode() !== $expectedCode) {
            throw new UserCommandFailed(
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
        $response = $this->httpClient->put(
            $uri,
            ['Content-Type' => 'application/json'],
            $content,
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() !== $expectedCode) {
            throw new UserCommandFailed(
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
        $response = $this->httpClient->post(
            $uri,
            ['Content-Type' => 'application/json'],
            $content,
            $userCredentials,
            static function (Throwable $e) {
                throw new EventStoreConnectionException($e->getMessage());
            }
        );

        if ($response->getStatusCode() === HttpStatusCode::CONFLICT) {
            throw new UserCommandConflictException($response->getStatusCode(), $response->getReasonPhrase());
        }

        if ($response->getStatusCode() !== $expectedCode) {
            throw new UserCommandFailed(
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

    private function createDateTimeString(string $dateTimeString): string
    {
        $micros = \substr($dateTimeString, 20, -1);
        $length = \strlen($micros);

        if ($length < 6) {
            $micros .= \str_repeat('0', 6 - $length);
        } elseif ($length > 6) {
            $micros = \substr($micros, 0, 6);
        }

        return \substr($dateTimeString, 0, 20) . $micros . 'Z';
    }
}
