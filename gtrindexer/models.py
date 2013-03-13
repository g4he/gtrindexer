import dao

class Project(dao.DomainObject):
    __type__ = "project"

class Person(dao.DomainObject):
    __type__ = "person"
    
class Organisation(dao.DomainObject):
    __type__ = "organisation"
    
class Publication(dao.DomainObject):
    __type__ = "publication"
