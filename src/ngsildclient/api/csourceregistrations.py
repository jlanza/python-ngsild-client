#!/usr/bin/env python3

# Software Name: ngsildclient
# SPDX-FileCopyrightText: Copyright (c) 2021 Orange
# SPDX-License-Identifier: Apache 2.0
#
# This software is distributed under the Apache 2.0;
# see the NOTICE file for more details.
#
# Author: Fabien BATTELLO <fabien.battello@orange.com> et al.

from __future__ import annotations
from typing import TYPE_CHECKING, Sequence
from functools import partialmethod
from hashlib import sha1

import logging

import re
import json

from ..model.constants import CORE_CONTEXT
from .exceptions import NgsiApiError

if TYPE_CHECKING:
    from .client import Client
from .exceptions import NgsiResourceNotFoundError, rfc7807_error_handle

from ngsildclient.api.helper.csourceregistration import CSourceRegistration

logger = logging.getLogger(__name__)

class CSourceRegistrations:
    """A wrapper for the NGSI-LD API subscriptions endpoint."""

    def __init__(self, client: Client, url: str):
        self._client = client
        self._session = client.session
        self.url = url

    @rfc7807_error_handle
    def register(self, csourcereg: CSourceRegistration, raise_on_conflict: bool = True) -> str:
        csourcereg_dict = csourcereg.to_dict()
        if raise_on_conflict:
            conflicts = self.conflicts(csourcereg_dict)
            if conflicts:
                raise ValueError(f"A csource already exists with same target : {conflicts}")
        r = self._session.post(f"{self.url}/", json=csourcereg_dict)
        self._client.raise_for_status(r)
        location = r.headers.get("Location")
        if location is None:
            if self._client.ignore_errors:
                return None
            else:
                raise NgsiApiError("Missing Location header")
        id_returned_from_broker = location.rsplit("/", 1)[-1]
        id = csourcereg_dict.get("id")
        if id is not None and id != id_returned_from_broker:
            raise NgsiApiError(f"Broker returned wrong id. Expected={id} Returned={id_returned_from_broker}")
        return id_returned_from_broker

    @rfc7807_error_handle
    def query(self, id: list[str], 
              type: str = None,
              idPattern: str = None, 
              attrs: str = None,
              q: str = None,
              csf: str = None,
              geometry: str = None,
              georel: str = None,
              coordinates : str = None,
              geoproperty : str = None,
              timeproperty : str = None,
              timerel  : str = None,
              timeAt : str = None,
              endTimeAt: str = None,
              geometryProperty: str = None,
              lang: str = None,
              scopeQ: str = None,
              ctx: str = CORE_CONTEXT) -> Sequence[CSourceRegistration]:

        if type is None and attrs is None and q is None and georel is None:
            raise ValueError("At least one of type, attrs, q or georel must be provided")
        if georel is not None and (geometry is None or coordinates is None):
            raise ValueError("geometry and coordinates must be provided if georel is provided")

        params = {}
        if isinstance(id, list) and len(id) > 0:
            params["id"] = ",".join(id)
        if type:
            params["type"] = type
        if idPattern:
            params["idPattern"] = idPattern
        if attrs:
            params["attrs"] = attrs
        if q:
            params["q"] = q
        if csf:
            params["csf"] = csf
        if geometry:
            params["geometry"] = geometry
        if georel:
            params["georel"] = georel
        if coordinates:
            params["coordinates"] = coordinates
        if geoproperty:
            params["geoproperty"] = geoproperty
        if timeproperty:
            if timeproperty not in ["observedAt", "createdAt", "modifiedAt", "deletedAt"]:
                raise ValueError("timeproperty must be one of observedAt, createdAt, modifiedAt or deletedAt")
            params["timeproperty"] = timeproperty
        if timerel:
            if timerel not in ["before", "after", "between"]:
                raise ValueError("timerel must be one of before, after or between")
            params["timerel"] = timerel
        if timeAt: 
            params["timeAt"] = timeAt
        if endTimeAt:
            params["endTimeAt"] = endTimeAt
        if geometryProperty:
            params["geometryProperty"] = geometryProperty
        if lang:
            params["lang"] = lang
        if scopeQ:
            params["scopeQ"] = scopeQ

        headers = {
            "Accept": "application/ld+json",
            "Content-Type": None,
        }  # overrides session headers
        if ctx is not None:
            headers["Link"] = f'<{ctx}>; rel="{CORE_CONTEXT}"; type="application/ld+json"'
        r = self._session.get(f"{self.url}", params=params, headers=headers)
        self._client.raise_for_status(r)
        csourceregistrations = r.json()
        logger.debug(f"{csourceregistrations=}")

        return csourceregistrations
        # return [CSourceRegistration.from_dict(csourceregistration) for enticsourceregistration in csourceregistrations]

    @staticmethod
    def _criteria_only(csourcereg: dict):
        # params = csourcereg.copy()
        # params.pop("id", None)
        # params.pop("registrationName", None)
        # params.pop("description", None)
        params = {}
        params["endpoint"] = csourcereg["endpoint"]
        params["information"] = csourcereg["information"]

        return params

    @staticmethod
    def _hash(csource: dict):
        criteria = CSourceRegistrations._criteria_only(csource)
        return sha1(json.dumps(criteria, sort_keys=True).encode("utf-8")).digest()

    # TODO: Analyse what to include in the hash and how to query
    @rfc7807_error_handle
    def conflicts(self, csource: dict, ctx: str = CORE_CONTEXT) -> list:
        return []
        # hashref = CSourceRegistrations._hash(csource)
        # headers = {
        #     "Accept": "application/ld+json",
        #     "Content-Type": None,
        # }  # overrides session headers
        # if ctx is not None:
        #     headers["Link"] = f'<{ctx}>; rel="{CORE_CONTEXT}"; type="application/ld+json"'
        # r = self.query()
        # return [x for x in r.json() if CSourceRegistrations._hash(x) == hashref]

    @rfc7807_error_handle
    def get(self, id: str, ctx: str = CORE_CONTEXT) -> dict:
        headers = {
            "Accept": "application/ld+json",
            "Content-Type": None,
        }  # overrides session headers
        if ctx is not None:
            headers["Link"] = f'<{ctx}>; rel="{CORE_CONTEXT}"; type="application/ld+json"'
        r = self._session.get(f"{self.url}/{id}", headers=headers)
        self._client.raise_for_status(r)
        return r.json()

    @rfc7807_error_handle
    def exists(self, id: str, ctx: str = CORE_CONTEXT) -> bool:
        try:
            payload = self.get(id, ctx)
            if payload:
                return "@context" in payload
        except NgsiResourceNotFoundError:
            return False
        return False

    @rfc7807_error_handle
    def delete(self, id: str) -> bool:
        r = self._session.delete(f"{self.url}/{id}")
        self._client.raise_for_status(r)
        return bool(r)
