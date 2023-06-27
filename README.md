
# Requirements

- docker
- docker-compose


1. Rodar o docker-compose

```
docker-compose -f docker-compose-CeleryExecutor.yml up -d
```

2. Conferir que a tabela foi criada, pode se conectar com um SGBD (Dbeaver). Editar conexão conforme parâmetros abaixo

```
host = localhost
port = 5432
as variáveis abaixo configurar conforme parametro enviroment do docker-compose
data_base, user_name e password
```

3. Abrir o localhost:8080 do browser > Admin > Variables > Localizar o arquivo variables.json na sua máquina
   clicar em import variables. Abaixo um exemplo, basta substituir com suas informações

   ```
   {
    "AWS_ACCESS_KEY_ID": "",
    "AWS_SECRET_ACCESS_KEY": "",
    "MONGO_HOST": "",
    "MONGO_DB": "",
    "MONGO_COLLECTION": "",
    "MONGO_USER": "",
    "MONGO_PASSWORD": ""
}
   ```

4. No SGBD aplicar uma query, pra checar persistência dos dados

```
select * from pnad;
```