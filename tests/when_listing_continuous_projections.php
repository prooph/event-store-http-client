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
use Prooph\EventStore\Projections\ProjectionDetails;
use Prooph\EventStore\Util\Guid;

class when_listing_continuous_projections extends TestCase
{
    use ProjectionSpecification;

    /** @var ProjectionDetails[] */
    private $result;
    /** @var string */
    private $projectionName;

    protected function given(): void
    {
        $this->projectionName = Guid::generateAsHex();
        $this->createContinuousProjection($this->projectionName);
    }

    protected function when(): void
    {
        $this->result = $this->projectionsManager->listContinuous($this->credentials);
    }

    /** @test */
    public function should_return_continuous_projections(): void
    {
        $this->execute(function () {
            $found = false;

            foreach ($this->result as $value) {
                if ($value->effectiveName() === $this->projectionName) {
                    $found = true;
                    break;
                }
            }

            $this->assertTrue($found);
        });
    }
}
