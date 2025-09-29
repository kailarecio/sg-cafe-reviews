"""
Script to load the most recent collected data
"""

import pandas as pd
import glob
import os

def load_latest_data():
    """Loads the most recent cafe and review data"""
    
    # Find latest files
    cafe_files = glob.glob('data/raw/cafes_*.parquet')
    review_files = glob.glob('data/raw/reviews_*.parquet')
    
    if not cafe_files or not review_files:
        raise FileNotFoundError("No data files found in data/raw/")
    
    # Sort and get most recent
    latest_cafes = sorted(cafe_files)[-1]
    latest_reviews = sorted(review_files)[-1]
    
    # Load
    cafes_df = pd.read_parquet(latest_cafes)
    reviews_df = pd.read_parquet(latest_reviews)
    
    print(f"Loaded {len(cafes_df)} cafes from {os.path.basename(latest_cafes)}")
    print(f"Loaded {len(reviews_df)} reviews from {os.path.basename(latest_reviews)}")
    
    return cafes_df, reviews_df

if __name__ == "__main__":
    cafes, reviews = load_latest_data()
    print("\nCafes by region:")
    print(cafes['region'].value_counts())
    print("\nSample reviews:")
    print(reviews[['place_name', 'rating', 'text']].head(3))