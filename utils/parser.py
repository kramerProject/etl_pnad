

def parse_people(raw_data):
    return [parse_person(person) for person in raw_data]

def parse_person(person):
    person["id"] = str(person["_id"])
    del person["_id"]
    return person