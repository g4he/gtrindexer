import dao

class Project(dao.DomainObject):
    __type__ = "project"

class CerifProject(dao.DomainObject):
    __type__ = "cerifproject"

class Person(dao.DomainObject):
    __type__ = "person"
    
class Organisation(dao.DomainObject):
    __type__ = "organisation"
    
class Publication(dao.DomainObject):
    __type__ = "publication"
    
class CerifClass(dao.DomainObject):
    __type__ = "cerifclass"
    
class Record(dao.DomainObject):
    __type__ = "record"
