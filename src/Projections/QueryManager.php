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

namespace Prooph\EventStoreHttpClient\Projections;

use Http\Message\RequestFactory;
use Prooph\EventStore\Projections\QueryManager as SyncQueryManager;
use Prooph\EventStore\UserCredentials;
use Prooph\EventStoreHttpClient\ConnectionSettings;
use Psr\Http\Client\ClientInterface;

/**
 * API for executing queries in the Event Store through PHP code.
 * Communicates with the Event Store over the RESTful API.
 *
 * Note: Configure the HTTP client with large enough timeout.
 */
class QueryManager implements SyncQueryManager
{
    /** @var ProjectionsManager */
    private $projectionsManager;

    /** @internal */
    public function __construct(
        ClientInterface $client,
        RequestFactory $requestFactory,
        ConnectionSettings $settings
    ) {
        $this->projectionsManager = new ProjectionsManager(
            $client,
            $requestFactory,
            $settings
        );
    }

    /**
     * Executes a query
     *
     * Creates a new transient projection and polls its status until it is Completed
     *
     * returns String of JSON containing query result
     *
     * @param string $name A name for the query
     * @param string $query The source code for the query
     * @param int $initialPollingDelay Initial time to wait between polling for projection status
     * @param int $maximumPollingDelay Maximum time to wait between polling for projection status
     * @param string $type The type to use, defaults to JS
     * @param UserCredentials|null $userCredentials Credentials for a user with permission to create a query
     *
     * @return string
     */
    public function execute(
        string $name,
        string $query,
        int $initialPollingDelay,
        int $maximumPollingDelay,
        string $type = 'JS',
        ?UserCredentials $userCredentials = null
    ): string {
        $this->projectionsManager->createTransient(
            $name,
            $query,
            $type,
            $userCredentials
        );

        $this->waitForCompleted(
            $name,
            $initialPollingDelay,
            $maximumPollingDelay,
            $userCredentials
        );

        return $this->projectionsManager->getState(
            $name,
            $userCredentials
        );
    }

    private function waitForCompleted(
        string $name,
        int $initialPollingDelay,
        int $maximumPollingDelay,
        ?UserCredentials $userCredentials
    ): void {
        $attempts = 0;
        $status = $this->getStatus($name, $userCredentials);

        while (false === \strpos($status, 'Completed')) {
            $attempts++;

            $this->delayPolling(
                $attempts,
                $initialPollingDelay,
                $maximumPollingDelay
            );

            $status = $this->getStatus($name, $userCredentials);
        }
    }

    private function delayPolling(
        int $attempts,
        int $initialPollingDelay,
        int $maximumPollingDelay
    ): void {
        $delayInMilliseconds = $initialPollingDelay * (2 ** $attempts - 1);
        $delayInMilliseconds = (int) \min($delayInMilliseconds, $maximumPollingDelay);

        \usleep($delayInMilliseconds * 1000);
    }

    private function getStatus(string $name, ?UserCredentials $userCredentials): string
    {
        return $this->projectionsManager->getStatus($name, $userCredentials);
    }
}
