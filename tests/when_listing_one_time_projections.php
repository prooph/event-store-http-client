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
use Prooph\EventStore\Projections\ProjectionDetails;

class when_listing_one_time_projections extends TestCase
{
    use ProjectionSpecification;

    /** @var ProjectionDetails[] */
    private $result;

    protected function given(): void
    {
        $this->createOneTimeProjection();
    }

    protected function when(): void
    {
        $this->result = $this->projectionsManager->listOneTime($this->credentials);
    }

    /** @test */
    public function should_return_continuous_projections(): void
    {
        $this->execute(function () {
            $this->assertNotEmpty($this->result);
        });
    }
}
