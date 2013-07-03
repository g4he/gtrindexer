from gtr import workflows
import models

mapping = {
    "mappings" : {
        "project" : {
            "dynamic_templates" : [
                {
                    "default" : {
                        "match" : "*",
                        "match_mapping_type": "string",
                        "mapping" : {
                            "type" : "multi_field",
                            "fields" : {
                                "{name}" : {"type" : "{dynamic_type}", "index" : "analyzed", "store" : "no"},
                                "exact" : {"type" : "{dynamic_type}", "index" : "not_analyzed", "store" : "yes"}
                            }
                        }
                    }
                }
            ]
        },
        "person" : {
            "dynamic_templates" : [
                {
                    "default" : {
                        "match" : "*",
                        "match_mapping_type": "string",
                        "mapping" : {
                            "type" : "multi_field",
                            "fields" : {
                                "{name}" : {"type" : "{dynamic_type}", "index" : "analyzed", "store" : "no"},
                                "exact" : {"type" : "{dynamic_type}", "index" : "not_analyzed", "store" : "yes"}
                            }
                        }
                    }
                }
            ]
        },
        "organisation" : {
            "dynamic_templates" : [
                {
                    "default" : {
                        "match" : "*",
                        "match_mapping_type": "string",
                        "mapping" : {
                            "type" : "multi_field",
                            "fields" : {
                                "{name}" : {"type" : "{dynamic_type}", "index" : "analyzed", "store" : "no"},
                                "exact" : {"type" : "{dynamic_type}", "index" : "not_analyzed", "store" : "yes"}
                            }
                        }
                    }
                }
            ]
        },
        "publication" : {
            "dynamic_templates" : [
                {
                    "default" : {
                        "match" : "*",
                        "match_mapping_type": "string",
                        "mapping" : {
                            "type" : "multi_field",
                            "fields" : {
                                "{name}" : {"type" : "{dynamic_type}", "index" : "analyzed", "store" : "no"},
                                "exact" : {"type" : "{dynamic_type}", "index" : "not_analyzed", "store" : "yes"}
                            }
                        }
                    }
                }
            ]
        }
    }
}

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
    pub = models.Publication(**publication.as_dict())
    print "saving data from " + publication.url()
    pub.save()

# create the mapping before we run
models.Project.create_mapping(mapping)

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
