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

namespace ProophTest\EventStoreHttpClient;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Util\Guid;

class when_disabling_projections extends TestCase
{
    use ProjectionSpecification;

    /** @var string */
    private $projectionName;
    /** @var string */
    private $streamName;
    /** @var string */
    private $query;

    public function given(): void
    {
        $id = Guid::generateAsHex();
        $this->projectionName = 'when_disabling_projection-' . $id;
        $this->streamName = 'test-stream-' . $id;

        $this->postEvent($this->streamName, 'testEvent', '{"A": 1}');
        $this->postEvent($this->streamName, 'testEvent', '{"A": 2}');

        $this->query = $this->createStandardQuery($this->streamName);

        $this->projectionsManager->createContinuous(
            $this->projectionName,
            $this->query,
            false,
            'JS',
            $this->credentials
        );
    }

    protected function when(): void
    {
        $this->projectionsManager->disable(
            $this->projectionName,
            $this->credentials
        );
    }

    /** @test */
    public function should_stop_the_projection(): void
    {
        $this->execute(function () {
            $projectionStatus = $this->projectionsManager->getStatus(
                $this->projectionName,
                $this->credentials
            );
            $status = $projectionStatus->status();

            $this->assertStringStartsWith('Stopped', $status);
        });
    }
}
