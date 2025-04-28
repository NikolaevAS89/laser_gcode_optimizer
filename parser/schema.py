from __future__ import annotations

import json
import logging
import re
from json import JSONEncoder
from typing import Any

from parser.stream import Stream

logger = logging.getLogger(__name__)


class ClassJSONEncoder(JSONEncoder):
    def default(self, obj):
        return obj.__dict__


class Metadata:
    """
     Simple metadata class to quick check changes in API.
     Main idea is combine all objects in parser in a one schema
     by use Metadata.merge() in reduce step. Then compare obtained schema
     with schema of last dump by using Metadata.diff(). Result ay not be great but
     can be first step in catch problems while dumping. It could be improved in the future.
    """
    __ENUM_MAX_SIZE__ = 16
    __BLOB_MIN_SIZE__ = 64

    def __init__(self,
                 obj_type: str,
                 schema: Any = None,
                 optional: bool = False,
                 nullable: bool = False):
        if isinstance(schema, dict):
            if "list" == obj_type:
                self.schema = Metadata(**schema)
            elif "dict":
                self.schema = dict()
                for key, value in schema.items():
                    self.schema[key] = value if isinstance(value, Metadata) else Metadata(**value)
            else:
                self.schema = schema
        else:
            self.schema = schema
        self.obj_type = obj_type
        self.optional = optional
        self.nullable = nullable

    @staticmethod
    def compute(obj: Any) -> Metadata:
        obj_type = re.sub(r"<class '(.+)'>", r'\1', str(type(obj)))
        if obj is None:
            '''
                <class 'NoneType'>
            '''
            return Metadata(obj_type=obj_type, schema=None, nullable=True)
        else:
            if isinstance(obj, list):
                '''
                    <class 'list'>
                '''
                if len(obj) == 0:
                    return Metadata(obj_type=obj_type, schema=None)
                else:
                    schema = Stream(obj).map(lambda item: Metadata.compute(item)) \
                        .reduce(Metadata.merge)
                    return Metadata(obj_type=obj_type, schema=schema)
            elif isinstance(obj, dict):
                '''
                    <class 'dict'>
                '''
                schema = dict()
                for key, value in obj.items():
                    schema[key] = Metadata.compute(value)
                return Metadata(obj_type=obj_type, schema=schema)
            else:
                '''
                    <class 'int'>
                    <class 'float'>
                    <class 'str'>
                '''
                schema = obj if isinstance(obj, str) and len(obj) <= Metadata.__BLOB_MIN_SIZE__ else None
                return Metadata(obj_type=obj_type, schema=schema)

    def to_dict(self) -> dict:
        if isinstance(self.schema, Metadata):
            return {"schema": self.schema.to_dict(),
                    "obj_type": self.obj_type,
                    "optional": self.optional,
                    "nullable": self.nullable}
        elif isinstance(self.schema, dict):
            return {"schema": Stream(self.schema.items()).map(lambda item: (item[0], item[1].to_dict())).to_dict(),
                    "obj_type": self.obj_type,
                    "optional": self.optional,
                    "nullable": self.nullable}
        else:
            return vars(self)

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), cls=ClassJSONEncoder)

    @staticmethod
    def from_dict(pyobj: dict) -> Metadata:
        return Metadata(**pyobj)

    @staticmethod
    def from_str(serialized: str) -> Metadata:
        pyobj = json.loads(serialized)
        return Metadata.from_dict(pyobj)

    @staticmethod
    def merge(obj1: Metadata, obj2: Metadata):
        if obj1 is None:
            return obj2
        elif obj2 is None:
            return obj1
        else:
            return obj1.enrich(obj2)

    def enrich(self, obj: Metadata) -> Metadata:
        """
        it can be used in a Stream.reduce function to gain common schema of Streams objects
        """
        self.optional = self.optional or obj.optional
        self.nullable = self.nullable or obj.nullable
        multiclass = self.obj_type is None or obj.obj_type is None

        if multiclass:
            values1 = self.schema if self.obj_type is None else [self.obj_type]
            values2 = obj.schema if obj.obj_type is None else [obj.obj_type]
            for value in values2:
                if value not in values1:
                    values1.append(value)
            self.schema = values1
            self.obj_type = None
        elif "str" == self.obj_type and "str" == obj.obj_type:
            if self.schema is None or obj.schema is None:
                self.schema = None
            else:
                is_array_1 = isinstance(self.schema, list)
                is_array_2 = isinstance(obj.schema, list)
                self.schema = self.schema if is_array_1 else [self.schema]
                obj.schema = obj.schema if is_array_2 else [obj.schema]
                for item in obj.schema:
                    if item not in self.schema:
                        self.schema.append(item)
                if len(self.schema) > Metadata.__ENUM_MAX_SIZE__:
                    self.schema = None
        elif "list" == self.obj_type and "list" == obj.obj_type:
            if self.schema is not None and obj.schema is not None:
                self.schema = self.schema.enrich(obj=obj.schema)
            elif self.schema is None and obj.schema is not None:
                obj.schema.optional = True
                self.schema = obj.schema
            elif self.schema is not None:
                self.schema.optional = True
        elif "dict" == self.obj_type and "dict" == obj.obj_type:
            result = dict()
            # update a values schemas
            for key in self.schema.keys():
                if key in obj.schema:
                    result[key] = self.schema.get(key).enrich(obj.schema.get(key))
                else:
                    value = self.schema.get(key)
                    value.optional = True
                    result[key] = value
            # enrich
            keys = set(obj.schema.keys()) - set(self.schema.keys())
            for key in keys:
                value = obj.schema.get(key)
                value.optional = True
                result[key] = value
            self.schema = result
        elif self.obj_type != obj.obj_type:
            if "NoneType" == self.obj_type:
                self.schema = obj.schema
                self.optional = obj.optional
                self.obj_type = obj.obj_type
                self.nullable = True
            elif "NoneType" == obj.obj_type:
                self.nullable = True
            else:
                self.schema = [self.obj_type, obj.obj_type]
                self.obj_type = None
        return self

    def diff(self, obj: Metadata) -> [str]:
        """
        make diff of two metadata represented as list of strings
        """
        result = list()

        if self.nullable != obj.nullable:
            result.append(f"!nullable: {str(self.nullable)} -> {str(obj.nullable)}")
        if self.optional != obj.optional:
            result.append(f"!optional: {str(self.optional)} -> {str(obj.optional)}")
        if self.obj_type is None:
            result.append(f"!is multiclass")
        # compute a diff schema
        if self.obj_type != obj.obj_type:
            result.append(f"!type: {obj.obj_type} -> {self.obj_type}")
        else:
            stype_1 = re.sub(r"<class '(.+)'>", r'\1', str(type(self.schema)))
            stype_2 = re.sub(r"<class '(.+)'>", r'\1', str(type(obj.schema)))
            if stype_1 != stype_2:
                result.append(f"!schema type:{stype_2}->{stype_1}")
            elif "list" == stype_1:
                elems_1 = set(self.schema)
                elems_2 = set(obj.schema)
                removed = elems_2 - elems_1
                if len(removed) > 0:
                    result.append(f"!removed:{str(removed)}")
                added = elems_1 - elems_2
                if len(added) > 0:
                    result.append(f"!added:{str(added)}")
            elif isinstance(self.schema, Metadata):
                subresult = [f".item{diff}" for diff in self.schema.diff(obj.schema)]
                if len(subresult) > 0:
                    result.extend(subresult)
            elif isinstance(self.schema, dict):
                # update a values schemas
                for key in self.schema.keys():
                    if key in obj.schema:
                        subresult = [f".{key}{diff}" for diff in self.schema.get(key).diff(obj.schema.get(key))]
                        if len(subresult) > 0:
                            result.extend(subresult)
                    else:
                        result.append(f".{key}!new")
                # enrich
                keys = set(obj.schema.keys()) - set(self.schema.keys())
                for key in keys:
                    result.append(f".{key}!deprecated")

        return result


class MetadataAggregator:
    def __init__(self):
        self.schema = None

    def merge(self, other):
        metadata = Metadata.compute(other)
        self.schema = metadata if not self.schema else Metadata.merge(self.schema, metadata)

    def get(self) -> Metadata:
        return self.schema
