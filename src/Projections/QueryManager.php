<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2020 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2018-2020 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStoreHttpClient\Projections;

use Http\Message\RequestFactory;
use Prooph\EventStore\Projections\QueryManager as SyncQueryManager;
use Prooph\EventStore\Projections\State;
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
    private ProjectionsManager $projectionsManager;

    /** @internal */
    public function __construct(
        ConnectionSettings $settings,
        ClientInterface $client,
        RequestFactory $requestFactory
    ) {
        $this->projectionsManager = new ProjectionsManager(
            $settings,
            $client,
            $requestFactory
        );
    }

    /**
     * Executes a query
     *
     * Creates a new transient projection and polls its status until it is Completed
     */
    public function execute(
        string $name,
        string $query,
        int $initialPollingDelay,
        int $maximumPollingDelay,
        string $type = 'JS',
        ?UserCredentials $userCredentials = null
    ): State {
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
