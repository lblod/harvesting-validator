# poc-app-harvesting-filtering-service
POC for filtering based on shacl shapes

- By default, it uses the application-profile located at `./example/config`

## Test the example

- `cd ./example`
- `docker-compose build`
- `docker-compose up`
- Wait that the migration service has finished
- Open postman
- Run the request below
- Check the database at `http://localhost:8890/sparql`
- check the generated files in `./example/filtering`

`POST localhost:8088/delta`
  
```
{
	"inserts": [
		{
			"subject": {
				"type":"",
				"value":"http://redpencil.data.gift/id/task/84ecbc30-7cc9-11eb-b493-2329471b3650",
				"datatype":""
			},
			"predicate": {
				"type":"",
				"value":"http://www.w3.org/ns/adms#status",
				"datatype":""
			},
			"object": {
				"type":"",
				"value":"http://redpencil.data.gift/id/concept/JobStatus/scheduled",
				"datatype":""
			}
		}
		
		],
	"deletes": []
}

```
