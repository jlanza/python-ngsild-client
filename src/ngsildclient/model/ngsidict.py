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
from collections import MutableMapping

import ngsildclient.model.entity as entity

from typing import Any, Union, List
from datetime import datetime
from geojson import Point, LineString, Polygon, MultiPoint
from scalpl import Cut

from ..utils import iso8601, url
from ..utils.urn import Urn
from .constants import *
from .exceptions import *

import json

"""This module contains the definition of the NgsiDict class.
"""


class NgsiDict(Cut, MutableMapping):
    """This class is a custom dictionary that backs an entity.

    Attr is used to build and hold the entity properties, as well as the entity's root.
    It's not exposed to the user but intended to be used by the Entity class.
    NgsiDict provides methods that allow to build a dictionary compliant with a NGSI-LD structure.

    See Also
    --------
    model.Entity
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getitem__(self, path: str):
        r = super().__getitem__(path)
        if isinstance(r, dict) and not isinstance(r, NgsiDict):
            r = NgsiDict(r)
        return r

    def __repr__(self):
        return dict.__repr__(self.data)

    def __ior__(self, prop: Mapping):
        prop = prop.data if isinstance(prop, NgsiDict) else prop
        self.data |= prop
        return self

    @property
    def value(self):
        if self["type"] == "Relationship":
            return self["object"]
        else:
            return self["value"]

    @value.setter
    def value(self, v: Any):
        if self["type"] == "Relationship":
            self["object"] = v
        else:
            self["value"] = v     

    @classmethod
    def _from_json(cls, payload: str):
        d = json.loads(payload)
        return cls(d)

    @classmethod
    def _load(cls, filename: str):
        with open(filename, "r") as fp:
            d = json.load(fp)
            return cls(d)

    def to_dict(self) -> dict:
        return self.data
        
    def to_json(self, indent=None) -> str:
        """Returns the dict in json format"""
        return json.dumps(self,  ensure_ascii=False, indent=indent, 
            default = lambda x: x.data if isinstance(x, NgsiDict) else str)

    def pprint(self, *args, **kwargs) -> None:
        """Returns the dict pretty-json-formatted"""
        entity.Entity.globalsettings.f_print(self.to_json(indent=2, *args, **kwargs))

    def _save(self, filename: str, indent=2):
        with open(filename, "w") as fp:
            json.dump(self, fp, ensure_ascii=False, indent=indent,
                default = lambda x: x.data if isinstance(x, NgsiDict) else str)

    def prop(self, name: str, value: AttrValue, **kwargs):
        if isinstance(value, List):
            self[name] = NgsiDict._m__build_property(value, **kwargs)
        else:
            self[name] = NgsiDict._build_property(value, **kwargs)
        return self[name]

    def gprop(self, name: str, value: str, **kwargs):
        self[name] = NgsiDict._build_geoproperty(value, **kwargs)
        return self[name]

    def tprop(self, name: str, value: str, **kwargs):
        self[name] = NgsiDict._build_temporal_property(value, **kwargs)
        return self[name]

    def rel(self, name: str, value: str, **kwargs):
        if isinstance(name, Rel):
            name = name.value
        self[name] = NgsiDict._build_relationship(value, **kwargs)
        return NgsiDict(self[name])

    @staticmethod
    def _process_observedat(observedat):
        date_str, temporaltype, _ = iso8601.parse(observedat)
        if temporaltype != TemporalType.DATETIME:
            raise NgsiDateFormatError(f"observedAt must be a DateTime : {date_str}")
        return date_str

    @staticmethod
    def _build_property(
        attrV: AttrValue,
        *,
        escape: bool = False,
    ) -> NgsiDict:
        property: NgsiDict = NgsiDict()
        value = attrV.value
        if isinstance(value, (int, float, bool, list, dict)):
            v = value
        elif isinstance(value, str):
            v = url.escape(value) if escape else value
        else:
            raise NgsiUnmatchedAttributeTypeError(f"Cannot map {type(value)} to NGSI type. {value=}")
        property["type"] = AttrType.PROP.value  # set type
        property["value"] = v  # set value
        if attrV.unitcode is not None:
            property[META_ATTR_UNITCODE] = attrV.unitcode
        if attrV.observedat is not None:
            property[META_ATTR_OBSERVED_AT] = NgsiDict._process_observedat(attrV.observedat)
        if attrV.datasetid is not None:
            property[META_ATTR_DATASET_ID] = Urn.prefix(attrV.datasetid)
        if attrV.userdata:
            property |= attrV.userdata
        return property

    @staticmethod
    def _m__build_property(
        values: List[AttrValue],
        *,
        attrtype: AttrType = AttrType.PROP
    ) -> NgsiDict:
        attrkey = "object" if attrtype == AttrType.REL else "value"
        property: List[NgsiDict] = []
        for v in values:
            if attrtype == AttrType.REL:
                v.value = Urn.prefix(v.value.id) if isinstance(v.value, entity.Entity) else Urn.prefix(v.value)
            p = NgsiDict()
            p["type"] = attrtype.value
            p[attrkey] = v.value
            p[META_ATTR_DATASET_ID] = Urn.prefix(v.datasetid)            
            if v.unitcode is not None:
                p[META_ATTR_UNITCODE] = v.unitcode
            if v.observedat is not None:
                p[META_ATTR_OBSERVED_AT] = NgsiDict._process_observedat(v.observedat)
            if v.userdata:
                p |= v.userdata
            property.append(p)
        return property

    @staticmethod
    def _build_geoproperty(
        value: NgsiGeometry,
        observedat: Union[str, datetime] = None,
        datasetid: str = None,
    ) -> NgsiDict:
        property: NgsiDict = NgsiDict()
        property["type"] = AttrType.GEO.value  # set type
        if isinstance(value, (Point, LineString, Polygon, MultiPoint)):
            geometry = value
        elif isinstance(value, tuple) and len(value) == 2:  # simple way for a location Point
            lat, lon = value
            geometry = Point((lon, lat))
        else:
            raise NgsiUnmatchedAttributeTypeError(f"Cannot map {type(value)} to NGSI type. {value=}")
        property["value"] = geometry  # set value
        if observedat is not None:
            property[META_ATTR_OBSERVED_AT] = NgsiDict._process_observedat(observedat)
        if datasetid is not None:
            property[META_ATTR_DATASET_ID] = Urn.prefix(datasetid)
        return property

    @staticmethod
    def _build_temporal_property(value: NgsiDate) -> NgsiDict:
        property: NgsiDict = NgsiDict()
        property["type"] = AttrType.TEMPORAL.value  # set type
        date_str, temporaltype, dt = iso8601.parse(value)
        v = {
            "@type": temporaltype.value,
            "@value": date_str,
        }
        property["value"] = v  # set value
        return property

    @staticmethod
    def _build_relationship(
        value: Union[str, List[str]],
        observedat: Union[str, datetime] = None,
        datasetid: str = None,
        userdata: NgsiDict = None,
    ) -> NgsiDict:
        property: NgsiDict = NgsiDict()
        property["type"] = AttrType.REL.value  # set type
        if isinstance(value, List):
            property["object"] = []
            for v in value:
                v = Urn.prefix(v.id) if isinstance(v, entity.Entity) else Urn.prefix(v)
                property["object"].append(v)
        else:
            property["object"] = Urn.prefix(value.id) if isinstance(value, entity.Entity) else Urn.prefix(value)
        if observedat is not None:
            property[META_ATTR_OBSERVED_AT] = NgsiDict._process_observedat(observedat)
        if datasetid is not None:
            property[META_ATTR_DATASET_ID] = Urn.prefix(datasetid)
            if userdata:
                property |= userdata
        return property

    # the methods below build named properties without attaching to the root dictionary

    @classmethod
    def _mkprop(cls, name: str, *args, **kwargs):
        attrV = AttrValue(*args, **kwargs)
        v = cls().prop(name, attrV)
        return NgsiDict({name: v})

    @classmethod
    def _mktprop(cls, name: str, *args, **kwargs):
        v = cls().tprop(name, *args, **kwargs)
        return NgsiDict({name: v})

    @classmethod
    def _mkgprop(cls, name, *args, **kwargs):
        v = cls().gprop(name, *args, **kwargs)
        return NgsiDict({name: v})

    @classmethod
    def _mkrel(cls, name, *args, **kwargs):
        v = cls().rel(name, *args, **kwargs)
        return NgsiDict({name: v})
