from gtr import workflows
import models

def project_handler(project):
    proj = models.Project(**project.as_dict())
    print "saving data from " + project.url()
    proj.save()
    
def person_handler(person):
    pers = models.Person(**person.as_dict())
    print "saving data from " + person.url()
    pers.save()
    
def organisation_handler(organisation):
    org = models.Organisation(**organisation.as_dict())
    print "saving data from " + organisation.url()
    org.save()
    
def publication_handler(publication):
    pub = models.Project(**publication.as_dict())
    print "saving data from " + publication.url()
    pub.save()
    
workflows.crawl("http://gtr.rcuk.ac.uk/", 
    project_limit=None, 
    person_limit=None, 
    organisation_limit=None, 
    publication_limit=None, 
    project_callback=project_handler, 
    person_callback=person_handler, 
    organisation_callback=organisation_handler, 
    publication_callback=publication_handler,
    min_request_gap=0
)
