import requests
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd

url = "http://localhost:9000/api/v1/bets"  # Ensure this matches the actual URL

try:
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    data = response.json()

    # Extract event names from the fetched data
    events = [item["name"] for item in data]
    print(f"Fetched {len(events)} events")  # Check if you got the expected number
except requests.exceptions.RequestException as e:
    print(f"Error fetching data from the API: {e}")
    events = []  # Fallback to an empty list if fetching fails

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
    threshold = 0.25
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
