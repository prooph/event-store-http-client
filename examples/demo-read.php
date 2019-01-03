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

namespace Prooph\EventStoreHttpClient;

use Prooph\EventStore\EventData;
use Prooph\EventStore\EventId;
use Prooph\EventStore\ExpectedVersion;
use Prooph\EventStore\Position;
use Prooph\EventStore\StreamMetadata;
use Prooph\EventStore\UserCredentials;

require __DIR__ . '/../vendor/autoload.php';

$connection = EventStoreConnectionFactory::create();

$slice = $connection->readStreamEventsForward(
    'foo-bar',
    10,
    2,
    true
);

\var_dump($slice);

$slice = $connection->readStreamEventsBackward(
    'foo-bar',
    10,
    2,
    true
);

\var_dump($slice);

$event = $connection->readEvent('foo-bar', 2, true);

\var_dump($event);

$m = $connection->getStreamMetadata('foo-bar');

\var_dump($m);

$r = $connection->setStreamMetadata('foo-bar', ExpectedVersion::ANY, new StreamMetadata(
    null, null, null, null, null, [
        'foo' => 'bar',
    ]
));

\var_dump($r);

$m = $connection->getStreamMetadata('foo-bar');

\var_dump($m);

$wr = $connection->appendToStream('foo-bar', ExpectedVersion::ANY, [
    new EventData(EventId::generate(), 'test-type', false, 'jfkhksdfhsds', 'meta'),
    new EventData(EventId::generate(), 'test-type2', false, 'kldjfls', 'meta'),
    new EventData(EventId::generate(), 'test-type3', false, 'aaa', 'meta'),
    new EventData(EventId::generate(), 'test-type4', false, 'bbb', 'meta'),
]);

\var_dump($wr);

$ae = $connection->readAllEventsForward(Position::start(), 2, false, new UserCredentials(
    'admin',
    'changeit'
));

\var_dump($ae);

$aeb = $connection->readAllEventsBackward(Position::end(), 2, false, new UserCredentials(
    'admin',
    'changeit'
));

\var_dump($aeb);
