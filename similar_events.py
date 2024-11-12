import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import time

time.sleep(10)

url = "http://localhost:9000/api/v1/bets" 

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
session.mount("http://", HTTPAdapter(max_retries=retries))

try:
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    data = response.json()
    
    # List of event names
    events = [item["name"] for item in data if "name" in item]
    
    # Dictionary mapping bet_id to name
    events_dict = {str(item["bet_id"]): item["name"] for item in data if "bet_id" in item and "name" in item}
    
    print("Fetched event names:", events)  
    print("Fetched events dictionary:", events_dict)
    
except requests.exceptions.RequestException as e:
    print(f"Error fetching data from the API: {e}")
    events = []
    events_dict = {}

"""
# Check if events list is empty
if not events:
    print("No events to process. Exiting the script.")
else:
    # Continue with TF-IDF vectorization and similarity calculation
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(events)
    cosine_similarities = cosine_similarity(tfidf_matrix)

    similarity_df = pd.DataFrame(cosine_similarities, index=events, columns=events)
    print("Cosine Similarity Matrix:\n", similarity_df)

    # Define a threshold for similarity
    threshold = 0.8
    similar_event_pairs = []

    # Find and store pairs with high similarity
    for i in range(len(events)):
        for j in range(i + 1, len(events)):  # Avoid duplicate pairs
            if cosine_similarities[i, j] >= threshold:
                similar_event_pairs.append((events[i], events[j], cosine_similarities[i, j]))

    # Print similar event pairs
    print("\nSimilar Event Pairs:")
    for event1, event2, score in similar_event_pairs:
        print(f"Event 1: {event1}\nEvent 2: {event2}\nSimilarity Score: {score:.2f}\n")
"""

similar_event_ids = []  # Initialize similar_event_ids list

if not events:
    print("No events to process. Exiting the script.")
else:
    # Continue with TF-IDF vectorization and similarity calculation
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(events)
    cosine_similarities = cosine_similarity(tfidf_matrix)

    similarity_df = pd.DataFrame(cosine_similarities, index=events, columns=events)
    print("Cosine Similarity Matrix:\n", similarity_df)

    # Define a threshold for similarity
    threshold = 0.8
    similar_event_pairs = []

    # Reverse the dictionary for name-to-bet_id lookup
    name_to_bet_id = {name: bet_id for bet_id, name in events_dict.items()}

    # Find and store pairs with high similarity
    for i in range(len(events)):
        for j in range(i + 1, len(events)):  # Avoid duplicate pairs
            if cosine_similarities[i, j] >= threshold:
                # Append names and similarity scores to similar_event_pairs
                similar_event_pairs.append((events[i], events[j], cosine_similarities[i, j]))
                
                # Append bet_id pairs to similar_event_ids
                similar_event_ids.append((name_to_bet_id[events[i]], name_to_bet_id[events[j]], cosine_similarities[i, j]))

    # Print similar event pairs by name
    print("\nSimilar Event Pairs (by name):")
    for event1, event2, score in similar_event_pairs:
        print(f"Event 1: {event1}\nEvent 2: {event2}\nSimilarity Score: {score:.2f}\n")

    # Print similar event pairs by bet_id
    print("\nSimilar Event Pairs (by bet_id):")
    for bet_id1, bet_id2, score in similar_event_ids:
        print(f"Bet ID 1: {bet_id1}\nBet ID 2: {bet_id2}\nSimilarity Score: {score:.2f}\n")




