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

namespace Prooph\EventStoreHttpClient;

require __DIR__ . '/../vendor/autoload.php';

$connection = EventStoreConnectionFactory::create();

$sl = $connection->readStreamEventsForward('food', 0, 100);

\var_dump($sl->events()[0]);
