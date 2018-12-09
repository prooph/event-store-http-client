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
use Prooph\EventStoreHttpClient\Exception\InvalidArgumentException;
use Prooph\EventStoreHttpClient\Exception\UserCommandFailedException;
use Prooph\EventStoreHttpClient\Http\EndpointExtensions;
use Prooph\EventStoreHttpClient\Http\HttpClient;
use Prooph\EventStoreHttpClient\UserCredentials;

class UsersManager
{
    /** @var UsersClient */
    private $client;
    /** @var EndPoint */
    private $endPoint;
    /** @var string */
    private $schema;
    /** @var UserCredentials|null */
    private $defaultUserCredentials;

    public function __construct(
        HttpClient $client,
        EndPoint $endPoint,
        string $schema = EndpointExtensions::HTTP_SCHEMA,
        ?UserCredentials $defaultUserCredentials = null
    ) {
        $this->client = new UsersClient($client);
        $this->endPoint = $endPoint;
        $this->schema = $schema;
        $this->defaultUserCredentials = $defaultUserCredentials;
    }

    public function enable(string $login, ?UserCredentials $userCredentials = null): void
    {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        $this->client->enable($this->endPoint, $login, $userCredentials, $this->schema);
    }

    public function disable(string $login, ?UserCredentials $userCredentials = null): void
    {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        $this->client->disable($this->endPoint, $login, $userCredentials, $this->schema);
    }

    /** @throws UserCommandFailedException */
    public function deleteUser(string $login, ?UserCredentials $userCredentials = null): void
    {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        $this->client->delete($this->endPoint, $login, $userCredentials, $this->schema);
    }

    /** @return UserDetails[] */
    public function listAll(?UserCredentials $userCredentials = null): array
    {
        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        return $this->client->listAll($this->endPoint, $userCredentials, $this->schema);
    }

    public function getCurrentUser(?UserCredentials $userCredentials = null): UserDetails
    {
        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        return $this->client->getCurrentUser($this->endPoint, $userCredentials, $this->schema);
    }

    public function getUser(string $login, ?UserCredentials $userCredentials = null): UserDetails
    {
        if (empty($login)) {
            throw new InvalidArgumentException('Login cannot be empty');
        }

        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        return $this->client->getUser($this->endPoint, $login, $userCredentials, $this->schema);
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

        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        $this->client->createUser(
            $this->endPoint,
            new UserCreationInformation(
                $login,
                $fullName,
                $groups,
                $password
            ),
            $userCredentials,
            $this->schema
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

        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        $this->client->updateUser(
            $this->endPoint,
            $login,
            new UserUpdateInformation($fullName, $groups),
            $userCredentials,
            $this->schema
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

        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        $this->client->changePassword(
            $this->endPoint,
            $login,
            new ChangePasswordDetails($oldPassword, $newPassword),
            $userCredentials,
            $this->schema
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

        $userCredentials = $userCredentials ?? $this->defaultUserCredentials;

        $this->client->resetPassword(
            $this->endPoint,
            $login,
            new ResetPasswordDetails($newPassword),
            $userCredentials,
            $this->schema
        );
    }
}
