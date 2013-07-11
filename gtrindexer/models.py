import es

class Project(es.DomainObject):
    __type__ = "project"

class CerifProject(es.DomainObject):
    __type__ = "cerifproject"

class Person(es.DomainObject):
    __type__ = "person"
    
class Organisation(es.DomainObject):
    __type__ = "organisation"
    
class Publication(es.DomainObject):
    __type__ = "publication"
    
class CerifClass(es.DomainObject):
    __type__ = "cerifclass"
    
class Record(es.DomainObject):
    __type__ = "record"
