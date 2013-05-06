import models, csv

query = {
    "query" : {
        "term" : { "projectComposition.organisation.name.exact" : "Brunel University" }
    },
    "from" : 0,
    "size" : 100
}

report = []

counter = 1
while True:
    print "request " + str(counter)
    counter += 1
    
    result = models.Project.query(q=query)
    compositions = [i.get("_source", {}) for i in result.get('hits', {}).get('hits', [])]
    if len(compositions) == 0:
        break
    query['from'] = query['from'] + query['size']
    
    for composition in compositions:
        people = [p for p in composition.get("projectComposition", {}).get("projectPerson", [])]
        fund = composition.get("projectComposition", {}).get("project", {}).get("fund", {})
        title = composition.get("projectComposition", {}).get("project", {}).get("title", "")
        id = composition.get("projectComposition", {}).get("project", {}).get("id", "")
        ref = composition.get("projectComposition", {}).get("project", {}).get("grantReference", "")
        
        pi = ""
        ci = ""
        for person in people:
            if person.get("coInvestigator", False):
                ci = person.get("name", "")
            if person.get("principalInvestigator", False):
                pi = person.get("name", "")

        funder = fund.get("funder", {}).get("name", "")
        value = fund.get("valuePounds", "-1")
        start = fund.get("start", "")
        end = fund.get("end", "")


        report.append(
            [
                id,
                pi,
                ci,
                funder,
                ref,
                title,
                str(value),
                start,
                end
            ]
        )

writer = csv.writer(open("report.csv", "wb"))
writer.writerow(["GtR ID", "PI", "CoI", "Funder", "Grant Reference", "Project Title", "Award Value", "Start", "End"])

for line in report:
    newline = ["" if l is None else l for l in line]
    writer.writerow([l.encode("utf-8") for l in newline])
    # print ", ".join([unicode(l) for l in line])
