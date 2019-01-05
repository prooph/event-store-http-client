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

namespace ProophTest\EventStoreHttpClient;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\Util\Guid;

class when_resetting_projections extends TestCase
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
        $this->projectionName = 'when_resetting_projections-' . $id;
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
        $this->projectionsManager->reset(
            $this->projectionName,
            $this->credentials
        );
    }

    /** @test */
    public function should_reset_the_projection(): void
    {
        $this->execute(function () {
            $projectionStatus = \json_decode(
                $this->projectionsManager->getStatus(
                    $this->projectionName,
                    $this->credentials
                ),
                true
            );
            $status = $projectionStatus['status'];

            $this->assertStringStartsWith('Preparing', $status);

            \usleep(500000);

            $projectionStatus = \json_decode(
                $this->projectionsManager->getStatus(
                    $this->projectionName,
                    $this->credentials
                ),
                true
            );
            $status = $projectionStatus['status'];

            $this->assertSame('Running', $status);
        });
    }
}
