<?php

/**
 * This file is part of `prooph/event-store-http-client`.
 * (c) 2018-2019 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2018-2019 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStoreHttpClient;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Util\Guid;

class when_updating_a_projection_query extends TestCase
{
    use ProjectionSpecification;

    /** @var string */
    private $projectionName;
    /** @var string */
    private $streamName;
    /** @var string */
    private $newQuery;

    protected function given(): void
    {
        $this->projectionName = 'when_updating_a_projection_query';
        $this->streamName = 'test-stream-' . Guid::generateAsHex();

        $this->postEvent($this->streamName, 'testEvent', '{"A": 1}');
        $this->postEvent($this->streamName, 'testEvent', '{"A": 2}');

        $originalQuery = $this->createStandardQuery($this->streamName);
        $this->newQuery = $this->createStandardQuery('DifferentStream');

        $this->projectionsManager->createContinuous(
            $this->projectionName,
            $originalQuery,
            false,
            'JS',
            $this->credentials
        );
    }

    protected function when(): void
    {
        $this->projectionsManager->updateQuery(
            $this->projectionName,
            $this->newQuery,
            false,
            $this->credentials
        );
    }

    /** @test */
    public function should_update_the_projection_query(): void
    {
        $this->execute(function () {
            $query = $this->projectionsManager->getQuery(
                $this->projectionName,
                $this->credentials
            );

            $this->assertEquals($this->newQuery, $query);
        });
    }
}
