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

namespace Prooph\EventStoreHttpClient\Exception;

use Prooph\EventStore\Exception\ProjectionCommandConflictException as BaseException;

class ProjectionCommandConflictException extends BaseException
{
    /** @var int */
    private $httpStatusCode;

    public function __construct(int $httpStatusCode, string $message)
    {
        $this->httpStatusCode = $httpStatusCode;

        parent::__construct($message);
    }

    public function httpStatusCode(): int
    {
        return $this->httpStatusCode;
    }
}