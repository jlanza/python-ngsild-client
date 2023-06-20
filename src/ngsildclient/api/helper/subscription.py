#!/usr/bin/env python3

# Software Name: ngsildclient
# SPDX-FileCopyrightText: Copyright (c) 2021 Orange UNICAN
# SPDX-License-Identifier: Apache 2.0
#
# This software is distributed under the Apache 2.0;
# see the NOTICE file for more details.
#
# Author: Fabien BATTELLO <fabien.battello@orange.com> et al.
# Author: Jorge LANZA <jlanza@tlmat.unican.es> et al.

from dataclasses import dataclass

from typing import (
    Literal,
    Sequence,
    Any,
    Union,
    List,
    Tuple,
    Optional,
    Mapping,
    Callable,
)

from datetime import datetime

from ngsildclient.utils import iso8601, url
from ngsildclient.model.constants import CORE_CONTEXT


@dataclass
class EntitySelector:
    type: str
    id: str = None
    id_pattern: str = None

    def to_dict(self) -> dict:
        d = {}
        d["type"] = self.type
        if self.id:
            d["id"] = self.id
        if self.id_pattern:
            d["idPattern"] = self.id_pattern
        return d


@dataclass
class Endpoint:
    uri: str
    accept: str = "application/ld+json"
    timeout: int = 0
    cooldown: int = 0
    receiver_info: dict = None
    notifier_info: dict = None

    def to_dict(self) -> dict:
        d = {}
        d["uri"] = self.uri
        d["accept"] = self.accept
        if self.timeout > 0:  # Has to be greater than 0
            d["timeout"] = self.timeout
        if self.cooldown > 0:  # Has to be greater than 0
            d["cooldown"] = self.cooldown
        if self.receiver_info:
            d["receiverInfo"] = [{k: v} for k, v in self.receiver_info.items()]
        if self.notifier_info:
            d["notifierInfo"] = [{k: v} for k, v in self.notifier_info.items()]


@dataclass
class NotificationParams:
    endpoint: Endpoint
    attrs: list[str] = None
    format: str = "normalized"
    sys_attrs: bool = False
    show_changes: bool = False

    def to_dict(self) -> dict:
        d = {}
        if self.attrs:
            d["attributes"] = self.attrs
        d["format"] = self.format
        if self.sys_attrs:  # By default is false
            d["sysAttrs"] = self.sys_attrs
        if self.show_changes:  # By default is false
            d["showChanges"] = self.show_changes
        d["endpoint"] = self.endpoint.to_dict()
        return d


@dataclass
class Subscription:
    notification: NotificationParams
    id: str = None
    type: str = "Subscription"
    name: str = None
    description: str = None
    entities: list[EntitySelector] = None  # id, idPattern, or type
    watched_attrs: list[str] = None
    notification_trigger: list[str] = None
    time_interval: int = 0
    query: str = None
    # TODO: create Geo JSON
    geo_query: str = None
    csf: str = None
    active: bool = True
    expires_at: Union[str, datetime] = None
    throttling: int = 0
    temporal_query: str = None
    scope: str = None
    lang: str = None
    ctx: str = CORE_CONTEXT

    def to_dict(self) -> dict:
        d = {}
        if self.id:
            d["id"] = self.id
        d["type"] = self.type
        if self.name:
            d["subscriptionName"] = self.name
        if self.description:
            d["description"] = self.description
        if self.entities:
            d["entities"] = self.entities
        if self.watched_attrs:
            d["watchedAttributes"] = self.watched_attrs
        if self.notification_trigger:
            d["notificationTrigger"] = self.notification_trigger
        if self.time_interval > 0 and not self.watched_attrs:
            d["timeInterval"] = self.time_interval
        if self.query:
            d["q"] = self.query
        if self.csf:
            d["csf"] = self.csf
        if self.expires_at:
            if isinstance(self.expires_at, datetime):
                d["expiresAt"] = iso8601.from_datetime(self.expires_at)
            else:
                d["expiresAt"] = self.expires_at
        if self.throttling > 0 and not self.time_interval:
            d["throttling"] = self.throttling
        if self.temporal_query:
            d["temporalQ"] = self.temporal_query
        if self.scope:
            d["scopeQ"] = self.scope
        if self.scope:
            d["lang"] = self.scope

        d["isActive"] = self.active
        d["notification"] = self.notification.to_dict()
        d["@context"] = self.ctx
        return d


class SubscriptionBuilder:
    def __init__(self, uri: str, receiver_headers: dict = None):
        notification = NotificationParams(
            Endpoint(uri=uri, receiver_info=receiver_headers)
        )
        self._subscr = Subscription(notification)
        self._subscr.entities = []

    def id(self, value: str):
        if not isinstance(value, str):
            raise ValueError("id shall be a string")
        self._subscr.id = value
        return self

    def name(self, value: str):
        if not isinstance(value, str):
            raise ValueError("name shall be a string")
        self._subscr.name = value
        return self

    def description(self, value: str):
        if not isinstance(value, str):
            raise ValueError("description shall be a string")
        self._subscr.description = value
        return self

    def select_entities(
        self, type: str, id: str = None, id_pattern: str = None
    ):
        if not isinstance(type, str):
            raise ValueError("EntitySelector type shall be a string")
        if id and not isinstance(id, str):
            raise ValueError("EntitySelector id shall be a string")
        if id_pattern and not isinstance(id_pattern, str):
            raise ValueError("EntitySelector idPattern shall be a string")
        self._subscr.entities.append(EntitySelector(type, id, id_pattern))
        return self

    def watch(self, value: list[str]):
        if not isinstance(value, list):
            raise ValueError("watchedAttributes shall be a list of strings")
        if value == []:
            raise ValueError("Empty array is not allowed")
        self._subscr.watched_attrs = value
        return self

    def query(self, value: str):
        if not isinstance(value, str):
            raise ValueError("query shall be a string")
        self._subscr.query = url.escape(value)
        return self

    def notif(self, value: list[str]):
        if not isinstance(value, list):
            raise ValueError("attribute names shall be a list of strings")
        if value == []:
            raise ValueError("Empty array is not allowed")
        self._subscr.notification.attrs = value
        return self

    def context(self, value: str):
        if not isinstance(value, str):
            raise ValueError("context shall be a string")
        self._subscr.ctx = value
        return self

    def build(self) -> dict:
        if self._subscr.entities == [] and self._subscr.watched_attrs is None:
            raise ValueError(
                "At least one of (a) entities or (b) watchedAttributes shall be present."
            )
        return self._subscr.to_dict()
