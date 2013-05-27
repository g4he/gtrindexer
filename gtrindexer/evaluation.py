import models, csv

query = { 
    "query" : { 
        "query_string" : { "query" : "*" }
    }, 
    "from" : 0, 
    "size" : 100
}

report = []

counter = 1
while True:
    print "request " + str(counter)
    counter += 1
    
    result = models.Organisation.query(q=query)
    
    orgs = [i.get("_source", {}) for i in result.get('hits', {}).get('hits', [])]
    if len(orgs) == 0:
        break
    query['from'] = query['from'] + query['size']
    
    for org in orgs:
        id = org.get("id")
        name = org.get('organisationOverview', {}).get('organisation', {}).get('name')
        grants = org.get('organisationOverview', {}).get('project', [])
        grant_count = len(grants)
        total_awarded = sum([grant.get('fund', {}).get('valuePounds', 0) for grant in grants])
        report.append([id, name, grant_count, total_awarded])

writer = csv.writer(open("evaluation.csv", "wb"))
writer.writerow(["GtR Org ID", "Org Name", "Number of Grants", "Total Value of Grants"])

for line in report:
    newline = ["" if l is None else l for l in line]
    writer.writerow([l.encode("utf-8") if hasattr(l, "encode") else l for l in newline])  
        
