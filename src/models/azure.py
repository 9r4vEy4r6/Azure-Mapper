from enum import Enum


class AzureResource:
    def __init__(self, id, name, type, location):
        self.id = id
        self.name = name
        self.type = type
        self.location = location


class ROLE(Enum):
    ASSIGNED_TO = "ASSIGNED_TO"
    HAS_ROLE = "HAS_ROLE"


class AzureRelationship:
    def __init__(self, source: str, target: str, relationship_type: ROLE, extra_properties={}):
        self.source = source
        self.target = target
        self.relationship_type = relationship_type
        self.extra_properties = extra_properties
