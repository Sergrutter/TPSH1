import requests
import urllib3

urllib3.disable_warnings()

url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"

payload={
  'scope': 'GIGACHAT_API_PERS'
}
headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Accept': 'application/json',
  'RqUID': '765b5d5d-daaf-46b4-9380-22568127ca6d',
  'Authorization': 'Basic MDE5YzhiYTAtZDRiOS03NmM1LTk0NmQtZjRhNWIyMWUzZTQxOjQzZWMxMzA3LTI5YWUtNGRiYS04YzI4LWQxNTgwYWY1ODU0OA=='
}

response = requests.request("POST", url, headers=headers, data=payload, verify=False)

print(response.text)