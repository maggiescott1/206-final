import requests

response = requests.get(
  url="https://api.predicthq.com/v1/events",
  headers={
    "Authorization": "Bearer J1USOybaPI69NwvzNepJs7IhI0OeQNiOwHCHtVQD",
    "Accept": "application/json"
  },
  params={
   "country": "US",
   "category": "concerts"
  }
)

print(response.json())