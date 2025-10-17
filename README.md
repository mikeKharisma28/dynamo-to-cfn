Convert AWS CLI describe-table JSON format file to CloudFormation JSON format file

Export table from AWS using AWS CLI describe-table:
```powershell
aws dynamodb describe-table --table-name YourTableName > table.json
```

Create venv using powershell:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

```

Run the script to convert describe-table JSON file to CloudFormation JSON format file:
```powershell
python dynamo_describe_to_cfn.py table.json -o balance-template.json
```
