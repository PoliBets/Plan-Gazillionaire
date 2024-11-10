# Notes from Rinna: Converts each event description into a vector representation using TF-IDF
# Then calculates cosine similarity between all pairs of event vectors
# Print event pairs that have similarity over 0.25

# Import necessary libraries
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd

# List of event descriptions from two datasets
events = [
    "Fed interest rates: December 2024", 
    "Fed decision in December",
    "Fed funds rate in December?",
    "Super Bowl Champion 2025",
    "Next Fed rate hike?",
    "How many Fed rate cuts this year?"
    "NBA Champion",
    "Arizona Senate Election Winner",
    "Who will win the Senate race in Arizona?",
    "Who will be inaugurated as President?",
    "Trump nominates Elon Musk to Cabinet?",
    "Elon Musk nominated to the Cabinet?",
    "Popular Vote Margin of Victory?",
    # Sample data for now
]

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
  
