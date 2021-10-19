#!/usr/bin/env python3

# Software Name: python-orion-client
# SPDX-FileCopyrightText: Copyright (c) 2021 Orange
# SPDX-License-Identifier: Apache 2.0
#
# This software is distributed under the Apache 2.0;
# see the NOTICE file for more details.
#
# Author: Fabien BATTELLO <fabien.battelo@orange.com> et al.
# SPDX-License-Identifier: Apache-2.0

import http
import logging

__version__ = "0.1.0"

from .utils import iso8601
from .model.entity import Entity
from .model.ngsidict import NgsiDict
from .api.client import Client
from .exceptions import NgsiError
from .model.exceptions import NgsiModelError
from .api.exceptions import NgsiApiError, NgsiContextBrokerError, NgsiAlreadyExistsError


__all__ = [
    "iso8601",
    "Entity",
    "NgsiDict",
    "Client",
    "NgsiError",
    "NgsiModelError",
    "NgsiApiError",
    "NgsiContextBrokerError",
    "NgsiAlreadyExistsError",
]

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)

http.client.HTTPConnection.debuglevel = 1


def print_to_log(*args):
    logger.debug(" ".join(args))


# monkey patch the http.client's print() function
http.client.print = print_to_log
