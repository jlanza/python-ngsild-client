#!/usr/bin/env python3

# Software Name: ngsildclient
# SPDX-FileCopyrightText: Copyright (c) 2021 UNICAN
# SPDX-License-Identifier: Apache 2.0
#
# This software is distributed under the Apache 2.0;
# see the NOTICE file for more details.
#
# Author: Jorge LANZA <jlanza@tlmat.unican.es> et al.

import inspect
from dataclasses import dataclass, fields

from dataclasses import dataclass

from datetime import datetime, timedelta
import isodate
from ngsildclient.utils import iso8601, url

import ngsildclient.utils.url as url
from ngsildclient.model.constants import CORE_CONTEXT

from geojson import Point, LineString, Polygon, MultiPoint
from geojson.geometry import Geometry

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

from enum import Enum, unique

@unique
class ImplementedOperation(Enum):
    # Context Information Provision  
    CREATE_ENTITY = "createEntity"
    UPDATE_ENTITY = "updateEntity"
    APPEND_ATTRS = "appendAttrs"
    UPDATE_ATTRS = "updateAttrs"
    DELETE_ATTRS = "deleteAttrs"
    DELETE_ENTITY = "deleteEntity"
    CREATE_BATCH = "createBatch"
    UPSERT_BATCH = "upsertBatch"
    UPDATE_BATCH = "updateBatch"
    DELETE_BATCH = "deleteBatch"
    UPSERT_TEMPORAL = "upsertTemporal"
    APPEND_ATTRS_TEMPORAL = "appendAttrsTemporal"
    DELETE_ATTRS_TEMPORAL = "deleteAttrsTemporal"
    UPDATE_ATTR_INSTANCE_TEMPORAL = "updateAttrInstanceTemporal"
    DELETE_ATTR_INSTANCE_TEMPORAL = "deleteAttrInstanceTemporal"
    DELETE_TEMPORAL = "deleteTemporal"
    MERGE_ENTITY = "mergeEntity"
    REPLACE_ENTITY = "replaceEntity"
    REPLACE_ATTRS = "replaceAttrs"
    MERGE_BATCH = "mergeBatch"

    # Context Information Consumption  
    RETRIEVE_ENTITY = "retrieveEntity"
    QUERY_ENTITY = "queryEntity"
    QUERY_BATCH = "queryBatch"
    RETRIEVE_TEMPORAL = "retrieveTemporal"
    QUERY_TEMPORAL = "queryTemporal"
    RETRIEVE_ENTITY_TYPES = "retrieveEntityTypes"
    RETRIEVE_ENTITY_TYPE_DETAILS = "retrieveEntityTypeDetails"
    RETRIEVE_ENTITY_TYPE_INFO = "retrieveEntityTypeInfo"
    RETRIEVE_ATTR_TYPES = "retrieveAttrTypes"
    RETRIEVE_ATTR_TYPE_DETAILS = "retrieveAttrTypeDetails"
    RETRIEVE_ATTR_TYPE_INFO = "retrieveAttrTypeInfo"

    # Context Information Subscription 
    CREATE_SUBSCRIPTION = "createSubscription"
    UPDATE_SUBSCRIPTION = "updateSubscription"
    RETRIEVE_SUBSCRIPTION = "retrieveSubscription"
    QUERY_SUBSCRIPTION = "querySubscription"
    DELETE_SUBSCRIPTION = "deleteSubscription"


@unique
class OperationsGroup(Enum):
    FEDERATION_OPS = "federationOps"
    UPDATE_OPS = "updateOps"
    RETRIEVE_OPS = "retrieveOps"
    REDIRECTION_OPS = "redirectionOps"


class TimeInterval:
    start_at: datetime = None
    end_at: datetime = None

    def __init__(
        self,
        start_at: datetime,
        end_at: datetime = None,
    ):

        if not isinstance(start_at, datetime) or (end_at  and not isinstance(end_at, datetime)):
            raise ValueError("start_at and end_at shall be datetime")
      
        self.start_at = iso8601.from_datetime(start_at)
        
        if end_at:
            self.end_at = iso8601.from_datetime(end_at)
    
    def to_dict(self) -> dict:
        d = {}
        d["startAt"] = self.start_at
        if self.end_at:
            d["endAt"] = self.end_at
        return d
        

@dataclass
class RegistrationManagementInfo:
    local_only: bool = None
    cache_duration: Union[str, datetime] = None
    timeout: int = None 
    cooldown: int = None

    def __init__(self, local_only: bool = None, cache_duration: timedelta = None, timeout: int = None, cooldown: int = None):
        if local_only:
            self.local_only = local_only
        if cache_duration:
            self.cache_duration = isodate.duration_isoformat(cache_duration)
        if timeout:
            if timeout < 0:
                raise ValueError("timeout shall be greater than 0")
            self.timeout = timeout
        if cooldown:
            if cooldown < 0:
                raise ValueError("cooldown shall be greater than 0")
            self.cooldonw = cooldown

    def to_dict(self) -> dict:
        d = {}
        if self.local_only:
            d["localOnly"] = self.local_only
        if self.cache_duration:
            d["cacheDuration"] = self.cache_duration
        if self.timeout:
            d["timeout"] = self.timeout
        if self.cooldonw:
            d["cooldown"] = self.cooldonw
        return d

class RegistrationInfo:
    class EntityInfo:
        id: str = None
        id_pattern: str = None
        type: Union[str, list[str]] = None

        def __init__(self, type: Union[str, list[str]], id_pattern: str = None, id: str = None):
            self.type = type
            if id_pattern:
                self.id_pattern = id_pattern
            if id:
                self.id = id

        def to_dict(self):
            d = {}
            d["type"] = self.type
            if (self.id):
                d["id"] = self.id
            if (self.id_pattern):
                d["idPattern"] = self.id_pattern
            return d

    entities: list[EntityInfo] = None
    property_names: list[str] = None
    relationship_names: list[str] = None

    def __init__(self, entities: list[EntityInfo] = None, property_names: list[str] = None, relationship_names: list[str] = None):
        if ((isinstance(entities, list) and len(entities) == 0) or 
            (isinstance(property_names, list) and len(property_names) == 0) or 
            (isinstance(relationship_names, list) and len(relationship_names) == 0)
        ):
            raise ValueError("entities, property_names and relationship_names shall not be empty lists")

        if entities:
            self.entities = entities
        if property_names:
            self.property_names = property_names
        if relationship_names:
            self.relationship_names = relationship_names

    def to_dict(self) -> dict:
        d = {}
        if self.entities:
            d["entities"] = [e.to_dict() for e in self.entities]
        if self.property_names:
            d["propertyNames"] = self.property_names
        if self.relationship_names:
            d["relationshipNames"] = self.relationship_names
        return d

@dataclass
class CSourceRegistration:
    endpoint: str = None
    information: list[RegistrationInfo] = None
    context: Union[str, list[str]] = None
    id: str = None
    type: str = "ContextSourceRegistration"
    registration_name: str = None
    description: str = None
    tenant: str = None
    observation_interval: TimeInterval = None
    management_interval: TimeInterval = None
    location: Geometry = None
    observation_space: Geometry = None
    operation_space: Geometry = None
    expires_at: str = None
    context_source_info: dict = None
    scope: Union[str, List[str]] = None
    mode: str = None
    operations: list[str] = None
    refresh_rate: Union[str, datetime] = None
    management: RegistrationManagementInfo = None
    other_properties: dict = None

    def to_dict(self) -> dict:
        d = {}
        if self.id:
            d["id"] = self.id
        d["type"] = self.type
        if self.registration_name:
            d["registrationName"] = self.registration_name
        if self.description:
            d["description"] = self.description
        if self.information:
            d["information"] = [i.to_dict() for i in self.information]
        if self.tenant:
            d["tenant"] = self.tenant
        if self.observation_interval:
            d["observationInterval"] = self.observation_interval.to_dict()
        if self.management_interval:
            d["managementInterval"] = self.management_interval.to_dict()
        if self.location:
            d["location"] = self.location
        if self.observation_space:
            d["observationSpace"] = self.observation_space
        if self.operation_space:
            d["operationSpace"] = self.operation_space
        if self.expires_at:
            d["expiresAt"] = self.expires_at
        d["endpoint"] = self.endpoint
        if self.context_source_info:
            d["contextSourceInfo"] = self.context_source_info
        if self.scope:
            d["scope"] = self.scope
        if self.mode:
            d["mode"] = self.mode
        if self.operations:
            d["operations"] = self.operations
        if self.refresh_rate:
            d["refreshRate"] = self.refresh_rate
        if self.management:
            d["management"] = self.management.to_dict()
        # TODO: Check to use JSON 
        if self.other_properties:
            for k, v in self.other_properties.items():
                d[k] = v
        d["@context"] = self.context
        return d
    
class CSourceRegistrationBuilder:
    def __init__(
        self,
        endpoint: str,
        information: Union[RegistrationInfo, list[RegistrationInfo]],
        context: Union[str, List[str]] = CORE_CONTEXT
    ):
        if not isinstance(endpoint, str) and not url.isurl(endpoint):
            raise ValueError("endpoint shall be a string matching a valid URL")
        
        if isinstance(information, RegistrationInfo):
            self._csourcereg = CSourceRegistration(endpoint, [ information ], context)
        elif isinstance(information, list):
            self._csourcereg = CSourceRegistration(endpoint, information, context)
        else:
            raise ValueError("information shall be a list of RegistrationInfo")

    def id(self, value: str):
        if not isinstance(value, str):
            raise ValueError("id shall be a string")
        self._csourcereg.id = value
        return self

    def registration_name(self, value: str):
        if not isinstance(value, str):
            raise ValueError("registration name shall be a string")
        self._csourcereg.name = value
        return self

    def description(self, value: str):
        if not isinstance(value, str):
            raise ValueError("description shall be a string")
        self._csourcereg.description = value
        return self

    def observation_interval(self, start_at: datetime, end_at: datetime = None):
        if not isinstance(start_at, datetime) or not isinstance(end_at, datetime):
            raise ValueError("start_at and end_at shall be datetime")
        self._csourcereg.observation_interval = TimeInterval(start_at, end_at)
        return self
    
    def management_interval(self, start_at: datetime, end_at: datetime = None):
        self._csourcereg.management_interval = TimeInterval(start_at, end_at)
        return self

    def location(self, value: Union[Tuple, Geometry]):
        if not isinstance(value, (tuple, Geometry)):
            raise ValueError("location shall be a tuple or Geometry")

        if isinstance(value, Tuple):
            if len(value) == 2:
                lat, lon = value
                value = Point((lon, lat))
            else:
                raise ValueError("lat, lon tuple expected")
        
        self._csourcereg.location = value
        return self

    def observation_space(self, value: Union[Tuple, Geometry]):
        if not isinstance(value, (tuple, Geometry)):
            raise ValueError("observation_space shall be a tuple or Geometry")

        if isinstance(value, Tuple):
            if len(value) == 2:
                lat, lon = value
                value = Point((lon, lat))
            else:
                raise ValueError("lat, lon tuple expected")
        
        self._csourcereg.observation_space = value
        return self

    def operation_space(self, value: Union[Tuple, Geometry]):
        if not isinstance(value, (tuple, Geometry)):
            raise ValueError("operation_space shall be a tuple or Geometry")

        if isinstance(value, Tuple):
            if len(value) == 2:
                lat, lon = value
                value = Point((lon, lat))
            else:
                raise ValueError("lat, lon tuple expected")
        
        self._csourcereg.operation_space = value
        return self

    def expires_at(self, value: datetime):
        if not isinstance(value, datetime):
            raise ValueError("expires_at shall be a datetime")
        self._csourcereg.expires_at = iso8601.from_datetime(value)
        return self
    
    def context_source_info(self, value: dict):
        if not isinstance(value, dict):
            raise ValueError("context_source_info shall be a dict, a generic key-value array")
        self._csourcereg.context_source_info = value
        return self
    
    def scope(self, value: Union[str, List[str]]):
        if not isinstance(value, str) and not isinstance(value, list):
            raise ValueError("scope shall be a string or a list of strings")
        self._csourcereg.scope = value
        return self

    def mode(self, value: Literal["inclusive", "exclusive", "redirect", "auxiliary"] = "inclusive"): 
        if not isinstance(value, str) and value not in ["inclusive", "exclusive", "redirect", "auxiliary"]:
            raise ValueError("mode shall be a string matching one of the following values: inclusive, exclusive, redirect, auxiliary")
        self._csourcereg.mode = value
        return self
    
    def operations(self, value: List[Union[ImplementedOperation, OperationsGroup]]):
        if not isinstance(value, list):
            raise ValueError("operations shall be a list of strings")
        self._csourcereg.operations = [op.value for op in value]
        return self
    
    def refresh_rate(self, value: timedelta):
        if not isinstance(value, timedelta):
            raise ValueError("refresh_rate shall be a timedelta")
        self._csourcereg.refresh_rate = isodate.duration_isoformat(value)
        return self

    def management(self, local_only: bool = None, cache_duration: timedelta = None, time: int = None, cooldown: int = None):
        self._csourcereg.management = RegistrationManagementInfo(local_only, cache_duration, time, cooldown)
        return self
    
    def other_properties(self, value: dict):
        if not isinstance(value, dict):
            raise ValueError("other_properties shall be a dict, a generic key-value array")
        self._csourcereg.other_properties = value
        return self

    def build(self) -> dict:
        if not self._csourcereg.endpoint or not self._csourcereg.information :
            raise ValueError("Both endpoint and information shall be present.")
        return self._csourcereg
