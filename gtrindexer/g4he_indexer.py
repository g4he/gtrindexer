import models, json, hashlib
from datetime import datetime

all_query = { 
    "query" : { 
        "query_string" : { "query" : "*" }
    }
}
query_size=1000

class_query = { 
    "query" : { 
        "query_string" : { "query" : "*" }
    }
}
class_page_size=500

role_map = {
    "PRINCIPAL_INVESTIGATOR" : "principalInvestigator",
    "CO_INVESTIGATOR" : "coInvestigator"
}

collaborator_people = [
    "principalInvestigator", "coInvestigator"
]

collaborator_orgs = [
    "leadRo", "fellow", "principalInvestigator", "coInvestigator"
]

mapping = {
    "record" : {
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
models.Record.type_mapping(mapping)

def _normalise(s):
    camel = "".join([w[0].upper() + w[1:] for w in s.lower().split(" ") if w != ""])
    return camel[0].lower() + camel[1:]

# load the cerif classes into memory for convenience
CERIF_CLASSES = {}
klazzs = models.CerifClass.query(q=class_query, result_size=class_page_size, raw=False)
for k in klazzs:
    cfclassid = k.get("cfClassId")
    value = None
    for jax in k.get("cfDescrOrCfDescrSrcOrCfTerm", []):
        name = jax.get("JAXBElement", {}).get("name")
        if name == "{urn:xmlns:org:eurocris:cerif-1.5-1}cfTerm":
            value = jax.get("JAXBElement", {}).get("value", {}).get("value")
            break
    CERIF_CLASSES[cfclassid] = _normalise(value)


# take the project record and bring the people up to the top level, under keys for their roles
def restructure_people(project):
    people = project.get('projectPerson', [])
    for person in people:
        # first look up the person's organisation in the index
        full_pers = models.Person.term("person.id.exact", person['id'], one_answer=True)
        org = full_pers.get("organisation")
        for role in person.get('projectRole'):
            mapped_role = role_map.get(role, role)
            full_record = {"person" : person, "organisation" : org}
            append(project, mapped_role, full_record)
            if mapped_role in collaborator_people:
                add_collaborator_person(project, mapped_role, full_record)
            if mapped_role in collaborator_orgs:
                add_collaborator_org(project, mapped_role, full_record.get("organisation"))

def add_collaborator_person(project, role, person):
    collp = {"person" :  person.get("person")}
    cname = canonical_name(person)
    if cname is None:
        return
    
    unique = unique_person_key(person)
    if duplicate_collaborator_person(project, unique):
        return
    
    collp['slug'] = unique
    collp['canonical'] = cname
    
    if role is "principalInvestigator":
        collp[unique + "_principalInvestigator"] = unique
        collp["principalInvestigator"] = cname
    
    if role is "coInvestigator":
        collp[unique + "_coInvestigator"] = unique
        collp["coInvestigator"] = cname
    
    append(project, "collaboratorPerson", collp)

def unique_person_key(person):
    p = person.get("person", {})
    o = person.get("organisation", {})
    s = p.get("firstName", "") + p.get("surname", "") + o.get("name", "")
    key = hashlib.md5(s.encode("utf-8")).hexdigest()
    return key

def duplicate_collaborator_person(project, unique):
    for cp in project.get("collaboratorPerson", []):
        if cp.get("slug") == unique:
            return True
    return False

def canonical_name(person):
    sn = person.get("person", {}).get("surname")
    fn = person.get("person", {}).get("firstName")
    if sn is not None and fn is not None:
         return sn + ", " + fn
    if sn is None and fn is not None:
        return fn
    if sn is not None and fn is None:
        return sn
    return None

def primary_funder(project):
    funder_id = project.get("project", {}).get("fund", {}).get("funder", {}).get("id")
    if funder_id is None:
        return
    full_org = models.Organisation.term("organisationOverview.organisation.id.exact", funder_id, one_answer=True)
    project['primaryFunder'] = full_org.get("organisationOverview", {}).get("organisation", {})

def restructure_orgs(project):
    # get the cerif record for the project
    pid = project.get("project", {}).get("id")
    cproj = models.CerifProject.term("cfClassOrCfClassSchemeOrCfClassSchemeDescr.cfProj.cfProjId.exact", pid, one_answer=True)
    
    # now mine the orgs, and cross-reference with the cerif data
    orgs = project.get("organisation", [])
    for org in orgs:
        # get the org's class id from the cerif project
        classids = _org_class_from_cerifproject(cproj, org['id'])
        
        for cid in classids:
            # look the class id up for it's human readable value
            classname = CERIF_CLASSES.get(cid)
            append(project, classname, org)
            if classname in collaborator_orgs:
                add_collaborator_org(project, classname, org)
    
    # now finally rationalise the lead research organisations data (this will
    # de-duplicate with any existing known leadRO)
    if "leadResearchOrganisation" in project:
        add_collaborator_org(project, "leadRo", project.get("leadResearchOrganisation"))

def add_collaborator_org(project, role, org):
    co = {"organisation" : org}
    cname, alts = org_names(org)
    
    # find out if this is a duplicate of an existing org record
    slug = unique_org_key(org, cname)
    if duplicate_collaborator_org(project, slug):
        return
    
    co['slug'] = slug
    co['canonical'] = cname
    co['alt'] = alts
    
    if role == "principalInvestigator":
        co[slug + "_principalInvestigator"] = slug
        co["principalInvestigator"] = cname
    
    if role == "coInvestigator":
        co[slug + "_coInvestigator"] = slug
        co["coInvestigator"] = cname
    
    if role == "leadRo":
        co[slug + "_leadRo"] = slug
        co["leadRo"] = cname
    
    if role == "fellow":
        co[slug + "_fellow"] = slug
        co["fellow"] = cname
    
    append(project, "collaboratorOrganisation", co)

def unique_org_key(org, cname):
    # we only really have the name to key off
    return hashlib.md5(cname.encode("utf-8")).hexdigest()
    
def duplicate_collaborator_org(project, slug):
    for o in project.get("collaboratorOrganisation", []):
        if o.get("slug") == slug:
            return True
    return False

def org_names(org):
    # FIXME: needs to bind to the alternate names api when that is ready
    name = org.get("name")
    return name, [name]

def cleanup(project):
    if "organisation" in project:
        del project['organisation']
    if "projectPerson" in project:
        del project['projectPerson']
    if "collaborator" in project:
        del project['collaborator']
    if "leadResearchOrganisation" in project:
        del project['leadResearchOrganisation']

def _org_class_from_cerifproject(cproj, org_id):
    cids = []
    descs = cproj.get("cfClassOrCfClassSchemeOrCfClassSchemeDescr", [])
    for desc in descs:
        for jax in desc.get("cfProj", {}).get("cfTitleOrCfAbstrOrCfKeyw", []):
            name = jax.get("JAXBElement", {}).get("name")
            if name == "{urn:xmlns:org:eurocris:cerif-1.5-1}cfProj_OrgUnit":
                ouid = jax.get("JAXBElement", {}).get("value", {}).get("cfOrgUnitId")
                if ouid == org_id:
                    cids.append(jax.get("JAXBElement", {}).get("value", {}).get("cfClassId"))
    return cids

def append(obj, key, value):
    if key in obj:
        obj[key].append(value)
    else:
        obj[key] = [value]

LIMIT = 5000
COUNTER = 1
fr = 0
while True:
    # list everything, and extract the objects
    projects = models.Project.query(q=all_query, from_record=fr, result_size=query_size, raw=False)
    
    # if we reach the end, break
    if len(projects) == 0:
        break
    
    # prep the next page query
    fr += query_size
    
    batch = []
    start = datetime.now()
    for project in projects:
        # unwrap from the unnecessary projectComposition
        project = project.get("projectComposition")
        
        # run all the restructuring tasks
        restructure_people(project)
        primary_funder(project)
        restructure_orgs(project)
        cleanup(project)
        
        # report to the cli
        # print str(COUNTER) + " batching enhanced record " + str(project.get("project", {}).get("id"))
        # print json.dumps(project, indent=2)
        batch.append(project)
        
        # save and iterate
        #record = models.Record(**project)
        #record.save()
        
        COUNTER += 1
    interim = datetime.now()
    processing_diff = interim - start
    interim_seconds = processing_diff.total_seconds()
    
    # print "WRITING BATCH TO ELASTIC SEARCH"
    models.Record.bulk(batch, refresh=True)
    
    end = datetime.now()
    diff = end - start
    total_seconds = diff.total_seconds()
    bulk_seconds = total_seconds - interim_seconds
    
    print ", ".join([str(COUNTER - 1), str(query_size), str(interim_seconds), str(bulk_seconds), str(total_seconds)])
    
    # if we have a limiter in place, determine if we need to break
    if LIMIT > 0 and COUNTER >= LIMIT:
        break

# when we finish, refresh the index
print "REFRESHING ELASTIC SEARCH"
models.Record.refresh()

