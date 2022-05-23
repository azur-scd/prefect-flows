from dotenv import dotenv_values
from neomodel import (config, db, StructuredNode, StructuredRel,StringProperty, IntegerProperty, UniqueIdProperty, DateProperty, DateTimeProperty, RelationshipTo, RelationshipFrom, Q, Traversal, OUTGOING, INCOMING, EITHER)
import datetime
env = dotenv_values("../.env")
#-----Neo4j database----- #
#config.DATABASE_URL = env["NEO4J_BOLT_URL"]
db.set_connection(env["NEO4J_BOLT_URL"])

class PublisherRelationship(StructuredRel):
    """
    A very simple relationship between two BasePersons that simply
    records the date at which an acquaintance was established.
    """
    on_date = DateProperty(default = datetime.datetime.now)

class PublicationhDateRelationship(StructuredRel):
    """
    A very simple relationship between two BasePersons that simply
    records the date at which an acquaintance was established.
    """
    on_date = DateProperty(default = datetime.datetime.now)

class OARelationship(StructuredRel):
    """
    A very simple relationship between two BasePersons that simply
    records the date at which an acquaintance was established.
    """
    on_date = DateProperty(default = datetime.datetime.now)

class AffiliationRelationship(StructuredRel):
    """
    A very simple relationship between two BasePersons that simply
    records the date at which an acquaintance was established.
    """
    on_date = DateProperty(default = datetime.datetime.now)

class ClassificationRelationship(StructuredRel):
    """
    A very simple relationship between two BasePersons that simply
    records the date at which an acquaintance was established.
    """
    on_date = DateProperty(default = datetime.datetime.now)

class StructureParentRelationship(StructuredRel):
    """
    A very simple relationship between two BasePersons that simply
    records the date at which an acquaintance was established.
    """
    on_date = DateProperty(default = datetime.datetime.now)

class Publication(StructuredNode):
    uid = UniqueIdProperty()
    doi = StringProperty(unique_index=True)
    scopus_id = StringProperty(index=True)
    hal_id = StringProperty(index=True)
    title = StringProperty(index=True,required=True)
    genre = StringProperty(index=True,required=True)
    publis_has_date = RelationshipTo("PublicationDate", "PUBLISHED_YEAR", model = PublicationhDateRelationship)
    publis_has_publisher = RelationshipTo("Publisher", "PUBLISHED_PUB", model = PublisherRelationship)
    publis_is_oa  = RelationshipTo("PublicationOA", "PUBLISHED_OA", model = OARelationship)
    publis_has_affiliation  = RelationshipTo("Structure", "IS_AFFILIATED", model = AffiliationRelationship)
    publis_has_classification  = RelationshipTo("BsoClass", "HAS_CLASS", model = ClassificationRelationship)
    def to_json(self):
        return {
        "id": self.id,
        "doi": self.doi,
        "scopus_id": self.scopus_id,
        "hal_id": self.hal_id,
        "title": self.title,
        "genre": self.genre,
        "group": "publications"
      }

class PublicationDate(StructuredNode):
    uid = UniqueIdProperty()
    value =  StringProperty(unique_index=True,required=True)
    def to_json(self):
        return {
        "id": self.id,
        "value": self.value,
        "group": "publication_date"
      }

class Publisher(StructuredNode):
    uid = UniqueIdProperty()
    doi_prefix =  StringProperty(unique_index=True,required=True)
    name = StringProperty(index=True,required=True)
    def to_json(self):
        return {
        "id": self.id,
        "doi_prefix": self.doi_prefix,
        "name": self.name,
        "group": "publisher"
      }

class PublicationOA(StructuredNode):
    uid = UniqueIdProperty()
    is_oa_value =  StringProperty(unique_index=True,required=True)
    host_type = StringProperty(unique_index=True,required=True)
    oa_status = StringProperty(unique_index=True,required=True)
    obs_date = StringProperty(unique_index=True,required=True)
    def to_json(self):
        return {
        "id": self.id,
        "is_oa_value": self.is_oa_value,
        "host_type": self.host_type,
        "oa_status": self.oa_status,
        "obs_date": self.obs_date,
        "group": "oa"
      }

class BsoClass(StructuredNode):
    uid = UniqueIdProperty()
    name_fr =  StringProperty(unique_index=True,required=True)
    name_en = StringProperty(unique_index=True,required=True)
    main_domain = StringProperty(required=True)
    def to_json(self):
        return {
        "id": self.id,
        "name_fr": self.name_fr,
        "name_en": self.name_en,
        "main_domain": self.main_domain,
        "group": "classification"
      }

class Structure(StructuredNode):
    uid = UniqueIdProperty()
    scopus_id = StringProperty(unique_index=True,required=True)
    ppn = StringProperty(unique_index=True)
    internal_id = IntegerProperty(unique_index=True,required=True)
    parent_id = IntegerProperty(unique_index=True,required=True)
    name = StringProperty(unique_index=True,required=True)
    structure_has_parent  = RelationshipTo("Structure", "HAS_PARENT", model = StructureParentRelationship)
    def to_json(self):
        return {
        "id": self.id,
        "scopus_id": self.scopus_id,
        "ppn": self.ppn,
        "internal_id": self.internal_id,
        "parent_id": self.parent_id,
        "name": self.name,
        "group": "structures"
      }