import json
import os
import json

import psycopg2
import boto3
import mysql.connector
from pymongo import MongoClient
from airflow.models import Variable

s3_client = boto3.client(
    's3',
    region_name='us-east-1',
    aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY")
)


with open("./data/ufs.json", 'r') as json_file:
    ufs = json.loads(json_file.read())


def extract_from_mongo():
    print("Extracting....from mongo")

    connection_string = f'mongodb+srv://{Variable.get("MONGO_USER")}:' \
                        f'{Variable.get("MONGO_PASSWORD")}@' \
                        f'{Variable.get("MONGO_HOST")}/' \
                        f'{Variable.get("MONGO_DB")}'
    
    client = MongoClient(connection_string)
    db = client.ibge
    collection = db.pnadc20203

    limit = 100

    results = collection.find().limit(limit)

    print("Parsing Results...ssss")
    people = parse_people(results)

    print("People-------------", people)

    with open("./data/raw-data.json", 'w') as json_file:
        json_file.write(json.dumps(people))

    client.close()

    return True


def load_raw_to_s3():
    print("Loading raw data to s3")
    s3_client.upload_file(
        "./data/raw-data.json",
        "dl-landing-zone-401868797180",
        "pnad/raw-data/pnad-raw.json"
    )
    print("Finishing")
    return True


def transform_data():
    print("Transforming")
    s3_client.download_file(
        "dl-landing-zone-401868797180",
        "pnad/raw-data/pnad-raw.json",
        "pnad-raw.json"
    )
    with open("pnad-raw.json", 'r') as json_file:
        raw = json.loads(json_file.read())

    
    out_generator = parse_full_people(raw)

    transformed = []
    for item in out_generator:
        transformed.append(item)

    with open("./data/transformed.json", 'w') as json_file:
        json_file.write(json.dumps(transformed))

    print("finished transform")
    return True


def load_to_dw():
    print("Load to dw")
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
    )


    cursor = conn.cursor()

    with open("./data/transformed.json", 'r') as json_file:
        data = json.loads(json_file.read())


    for item in data:
        # print(item)
        query = """INSERT INTO pnad (id, ano, trimestre, estado, uf, sexo, idade, cor, graduacao, trab, ocup, renda, horastrab, anosesco)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        values = (
            item['id'], item['ano'], item['trimestre'], item['estado'], item['uf'],
            item['sexo'], item['idade'], item['cor'], item['graduacao'], item['trab'],
            item['ocup'], item['renda'], item['horastrab'], item['anosesco']
        )
        cursor.execute(query, values)

    conn.commit()


    cursor.close()
    conn.close()

    print("load success")
    return True


def parse_people(raw_data):
    return [parse_person(person) for person in raw_data]

def parse_person(person):
    person["id"] = str(person["_id"])
    del person["_id"]
    return person


def parse_full_people(raw):
    return (parse_full_person(person) for person in raw)


def parse_full_person(person):
    return {
        "id": _parse_str(person.get("id", "")),
        "ano": _parse_int(person.get("ano", 0)),
        "trimestre": _parse_int(person.get("trimestre", 0)),
        "estado": _parse_str(person.get("uf", "")),
        "uf": _parse_uf(person.get("uf", "")),
        "sexo": _parse_str(person.get("sexo", "")),
        "idade": _parse_int(person.get("idade", 0)),
        "cor": _parse_str(person.get("cor", "")),
        "graduacao": _parse_str(person.get("graduacao", "")),
        "trab": _parse_str(person.get("trab", "")),
        "ocup": _parse_str(person.get("ocup", "")),
        "renda": _parse_float(person.get("renda")),
        "horastrab": _parse_float(person.get("horastrab")),
        "anosesco": _parse_float(person.get("anosesco"))
    }

def _parse_int(val):
    return val or 0

def _parse_str(val):
    return val or ""

def _parse_float(val):
    return val or None

def _parse_uf(uf):
    return [item["uf"] for item in ufs if item["estado"] == uf][0]