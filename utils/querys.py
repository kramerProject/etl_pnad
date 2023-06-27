from pymongo import MongoClient
import parser
import json


# String de conexão do cluster do MongoDB
connection_string = "mongodb+srv://estudante_igti:SRwkJTDz2nA28ME9@unicluster.ixhvw.mongodb.net/ibge"

# Conectar ao cluster
client = MongoClient(connection_string)

# Acessar um banco de dados
db = client.ibge


collection = db.pnadc20203


query = {
    "sexo": "Mulher",
    "uf": {"$in": ["Paraná", "Santa Catarina", "Rio Grande do Sul"]},
    "idade": {"$gte": 25, "$lte": 35},
    "trab": "Pessoas na força de trabalho"
}

# Executar a consulta de média de renda
result = collection.aggregate([
    {"$match": query},
    {"$group": {"_id": None, "average_income": {"$avg": "$renda"}}}
])

# Obter o resultado da média de renda
average_income = next(result)["average_income"]

# Exibir o resultado
print("Average Income of Women (Age 20-40):", average_income)

# Fechar a conexão
client.close()