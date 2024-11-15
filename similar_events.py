import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import openai
import os
import pandas as pd
import time
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
from openai.error import RateLimitError

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

time.sleep(10)

url = "http://localhost:9000/api/v1/bets" 

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
session.mount("http://", HTTPAdapter(max_retries=retries))

try:
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    data = response.json()
    events = [item["name"] for item in data if "name" in item]
    print("Fetched event names:", events)  
except requests.exceptions.RequestException as e:
    print(f"Error fetching data from the API: {e}")
    events = []

if not events:
    print("No events to process. Exiting the script.")
else:
    def get_event_embedding(event, max_retries=5, wait_seconds=10):
        retries = 0
        while retries < max_retries:
            try:
                response = openai.Embedding.create(
                    input=event,
                    model="text-embedding-ada-002"
                )
                return response['data'][0]['embedding']
            except RateLimitError:
                print(f"Rate limit exceeded. Retrying in {wait_seconds} seconds...")
                time.sleep(wait_seconds)
                retries += 1
        print(f"Failed to retrieve embedding for event after {max_retries} retries.")
        return None

    event_embeddings = [get_event_embedding(event) for event in events if get_event_embedding(event) is not None]

    valid_events = [event for event, embedding in zip(events, event_embeddings) if embedding is not None]
    if not valid_events:
        print("No valid embeddings were retrieved. Exiting the script.")
    else:
        cosine_similarities = cosine_similarity(event_embeddings)

        similarity_df = pd.DataFrame(cosine_similarities, index=valid_events, columns=valid_events)
        print("Cosine Similarity Matrix:\n", similarity_df)

        threshold = 0.8
        similar_event_pairs = []

        for i in range(len(valid_events)):
            for j in range(i + 1, len(valid_events)):  
                if cosine_similarities[i, j] >= threshold:
                    similar_event_pairs.append((valid_events[i], valid_events[j], cosine_similarities[i, j]))

        print("\nSimilar Event Pairs:")
        for event1, event2, score in similar_event_pairs:
            print(f"Event 1: {event1}\nEvent 2: {event2}\nSimilarity Score: {score:.2f}\n")





