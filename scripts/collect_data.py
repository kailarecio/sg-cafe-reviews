"""
Full data collection script for cafes across 12 neighborhoods in Singapore
Saves to data/raw/ as parquet files
"""

import logging
from datetime import datetime
import pandas as pd
import time
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.query import CafeQuery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def collect_all_data():
    """Collect cafes and reviews for all neighborhoods"""
    
    # Initialize
    logger.info("Initializing CafeQuery...")
    cafe_query = CafeQuery()
    
    # Placeholders
    all_cafes_dfs = []
    all_reviews_dfs = []
    
    # Process each neighborhood
    for i, neighborhood in enumerate(cafe_query.neighborhoods, 1):
        logger.info(f"[{i}/{len(cafe_query.neighborhoods)}] Processing {neighborhood['neighborhood_name']} ({neighborhood['region']})")
        
        # Collect cafes
        cafes_df = cafe_query.collect_cafes(neighborhood)
        logger.info(f"  Found {len(cafes_df)} cafes")
        
        if not cafes_df.empty:
            # Collect reviews for these cafes
            place_ids = cafes_df['place_id'].tolist()
            reviews_df = cafe_query.collect_reviews(place_ids)
            
            # Add neighborhood info to reviews
            reviews_df = reviews_df.merge(
                cafes_df[['place_id', 'neighborhood', 'region']],
                on='place_id',
                how='left'
            )
            
            logger.info(f"  Collected {len(reviews_df)} reviews")
            
            all_cafes_dfs.append(cafes_df)
            all_reviews_dfs.append(reviews_df)
        
        # Brief pause between neighborhoods
        time.sleep(1)
    
    # Combine all data
    logger.info("Combining all data...")
    final_cafes = pd.concat(all_cafes_dfs, ignore_index=True)
    final_reviews = pd.concat(all_reviews_dfs, ignore_index=True)
    
    # Remove duplicates (cafes might appear in overlapping neighborhoods)
    initial_cafe_count = len(final_cafes)
    final_cafes = final_cafes.drop_duplicates(subset=['place_id'])
    logger.info(f"Removed {initial_cafe_count - len(final_cafes)} duplicate cafes")
    
    # Save to parquet
    timestamp = datetime.now().strftime('%Y%m%d')
    
    # Ensure data/raw directory exists
    os.makedirs('data/raw', exist_ok=True)
    
    cafes_path = f'data/raw/cafes_{timestamp}.parquet'
    reviews_path = f'data/raw/reviews_{timestamp}.parquet'
    
    final_cafes.to_parquet(cafes_path)
    final_reviews.to_parquet(reviews_path)
    
    # Log summary
    logger.info("=" * 50)
    logger.info("Collection complete!")
    logger.info(f"Total unique cafes: {len(final_cafes)}")
    logger.info(f"Total reviews: {len(final_reviews)}")
    logger.info(f"Files saved:")
    logger.info(f"  - {cafes_path}")
    logger.info(f"  - {reviews_path}")
    logger.info("=" * 50)
    
    return final_cafes, final_reviews

if __name__ == "__main__":
    try:
        cafes, reviews = collect_all_data()
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        raise