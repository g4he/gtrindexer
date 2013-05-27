from gtr import workflows
import models
import json

def indexer(nproj, cproj):
    # get the data structure that we will index
    d = nproj.as_dict()
    d = d['projectComposition']
    
    # for each person in the project, add their person record 
    d['ci'] = []
    d['pi'] = []
    for person in nproj.people():
        pr = person.get_full()
        pd = pr.as_dict()
        del pd['projectComposition']
        if person.isPI():
            # if the person is the PI, add their full record (which includes org data)
            # to the pi field
            d['pi'].append(pd)
        elif person.isCI():
            d['ci'].append(pd)
    
    # add an org record for the funder
    f = nproj.funder()
    f.fetch()
    fd = f.as_dict()
    fd = fd['organisationOverview']['organisation']
    d['primary_funder'] = fd
    d['funders'] = [fd]
    
    # for each org, check its relation to the main record, and then
    # restructure the object appropriately
    for org in nproj.orgs():
        od = org.as_dict()
        od = od.get("organisationOverview", {}).get("organisation", {})
        rels = cproj.org_cerif_relations(org.id())
        for rel in rels:
            cc = rel.get_class()
            key = cc.term()
            key = key.lower().strip().replace(" ", "_")
            if key == "lead_ro":
                continue # this is already covered in the data
            if key in d:
                d[key].append(od)
            else:
                d[key] = [od]
    
    domainobj = models.Project(**d)
    print "saving data from " + nproj.url()
    domainobj.save()
    

workflows.crawl("http://gtr.rcuk.ac.uk/", 
                project_callback=indexer, 
                project_limit=10, 
                pass_cerif_project=True)
