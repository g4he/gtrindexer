from gtr import workflows
import models
import json

COUNTER = 1

def indexer(nproj, cproj):
    global COUNTER
    
    # get the data structure that we will index
    d = nproj.as_dict()
    d = d['projectComposition']
    
    # for each person in the project, add their person record 
    for person in nproj.people():
        pr = person.get_full()
        pd = pr.as_dict()
        del pd['projectComposition']
        if person.isPI():
            # if the person is the PI, add their full record (which includes org data)
            # to the pi field
            add_related_obj(d, "principalInvestigator", pd)
        elif person.isCI():
            add_related_obj(d, "coInvestigator", pd)
        
        # add the other roles
        roles = person.get_project_roles()
        for role in roles:
            if role != "CO_INVESTIGATOR" and role != "PRINCIPAL_INVESTIGATOR":
                add_related_obj(d, role, pd)
    
    # add an org record for the funder
    f = nproj.funder()
    f.fetch()
    fd = f.as_dict()
    fd = fd['organisationOverview']['organisation']
    d['primaryFunder'] = fd
    
    # for each org, check its relation to the main record, and then
    # restructure the object appropriately
    for org in nproj.orgs():
        od = org.as_dict()
        od = od.get("organisationOverview", {}).get("organisation", {})
        rels = cproj.org_cerif_relations(org.id())
        if rels is None or len(rels) == 0:
            add_related_obj(d, "unspecifiedRelationship", od)
        else:
            for rel in rels:
                cc = rel.get_class()
                key = cc.term()
                key = get_org_key(key)
                add_related_obj(d, key, od)
    
    # get rid of the original fields that we don't want to care about
    del d['organisation']
    del d['projectPerson']
    
    domainobj = models.Project(**d)
    print str(COUNTER) + " saving data from " + nproj.url()
    domainobj.save()
    COUNTER += 1

def add_related_obj(d, key, obj):
    if key is not None:
        if key in d:
            d[key].append(obj)
        else:
            d[key] = [obj]

def get_org_key(key):
    key = key.strip()
    if key == "Lead RO":
        return None
    elif key == "Fellow":
        return "fellow"
    elif key == "Other":
        return "other"
    elif key == "Co-Funder":
        return "coFunder"
    elif key == "Project Partner":
        return "projectPartner"
    return key.replace(" ", "")


workflows.crawl("http://gtr.rcuk.ac.uk/", 
                project_callback=indexer, 
                project_limit=1000, 
                pass_cerif_project=True)
