from gtrclient import workflows
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
    print "saving data from " + org.url()
    org.save()
    
def publication_handler(publication):
    pub = models.Project(**publication.as_dict())
    print "saving data from " + pub.url()
    pub.save()
    
workflows.crawl("http://gtr.rcuk.ac.uk/", 
    project_limit=200, 
    person_limit=200, 
    organisation_limit=200, 
    publication_limit=200, 
    project_callback=project_handler, 
    person_callback=person_handler, 
    organisation_callback=organisation_handler, 
    publication_callback=publication_handler,
    min_request_gap=0
)
