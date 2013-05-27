import models, csv

query = { 
    "query" : { 
        "query_string" : { "query" : "*" }
    }, 
    "from" : 0, 
    "size" : 500
}

report_dict = {}

counter = 1
while True:
    print "request " + str(counter)
    counter += 1
    
    result = models.Project.query(q=query)
    
    projects = [i.get("_source", {}) for i in result.get('hits', {}).get('hits', [])]
    if len(projects) == 0:
        break
    query['from'] = query['from'] + query['size']
    
    for project in projects:
        org_id = project.get("projectComposition", {}).get("leadResearchOrganisation", {}).get("id")
        org_name = project.get("projectComposition", {}).get("leadResearchOrganisation", {}).get('name')
        value = project.get("projectComposition", {}).get("project", {}).get("fund", {}).get("valuePounds")
        if org_id in report_dict:
            report_dict[org_id][2] += 1
            report_dict[org_id][3] += value
        else:
            report_dict[org_id] = [org_id, org_name, 1, value]

report = report_dict.values()

writer = csv.writer(open("evaluation_leadonly.csv", "wb"))
writer.writerow(["GtR Org ID", "Lead Org Name", "Number of Grants", "Total Value of Grants"])

for line in report:
    newline = ["" if l is None else l for l in line]
    writer.writerow([l.encode("utf-8") if hasattr(l, "encode") else l for l in newline])  
        
