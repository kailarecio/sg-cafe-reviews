import pandas as pd
import numpy as np
from typing import Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CafeProcessor:
    """pre-processing pipeline for google maps cafe reviews metadata"""

    def __init__(self):
        self.cafes_df = None
        self.reviews_df = None

    def load_data(self, cafe_path: str, reviews_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]
        """load raw data files"""
        self.cafes_df = pd.read_pickle(cafe_path)
        self.reviews_df = pd.read_pickle(reviews_path)
        logger.info(f"Loaded {len(self.cafes_df)} cafes and {len(self.reviews_df)} reviews")
        return self.cafes_df, self.reviews_df
    
    def clean_cafe_df(self) -> pd.DataFrame:
        """clean and process cafe df"""
        df = self.cafes_df.copy()

        # drop duplicates
        initial_len = len(df)
        df = df.drop_duplicates(subset=['place_id'])
        logger.info(f"Removed {initial_len - len(df) } duplicate cafes")

        # missing values -- generalize to if numeric and empty, fill with -1
        df[['price_level', 'rating', 'total_ratings']] = df[['price_level', 'rating', 'total_ratings']].fillna(-1)

        # arrange number of ratings by percentile
        df['ratings_pct'] = pd.qcut(
            df['total_ratings'],
            q = [0, 0,25, 5, 0,75, 1],
            labels = [1, 2, 3, 4]
        )

        self.cafes_df = df
        return df
    
    def clean_review_df(self) -> pd.DataFrame:
        """clean and process review df"""
        df = self.reviews_df.copy()

        # drop duplicates
        initial_len = len(df)
        df = df.drop_duplicates(subset=['place_id'])
        logger.info(f"Removed {initial_len - len(df) } duplicate cafes")

        # clean text
        df['text_clean'] = df['text'].apply(self.clean_review_text)

        # add text features
        df['text_length'] = df['text_clean'].str.len()
        df['word_count'] = df['text_clean'].str.split().str.len()

        # handle timestamps
        df['review_date'] = pd.to_datetime(df['time'], unit='s')
        df['year'] = df['review_date'].dt.year
        df['month'] = df['review_date'].dt.month
        df['day_of_week'] = df['review_date'].dt.dayofweek

        # flag potential fake reviews
        self.reviews_df = df
        return df

    
    def clean_review_text(self, text: str) -> str:
        """clean and process individual reviews"""

        # handle no text reviews written
        if pd.isna(text) or text == '':
            return ''
        
        text = text.lower()
        text = ' '.join(text.split())

        return text
    
    def cafe_reviews_df(self) -> pd.DataFrame:
        """Merge cafe and reviews data"""

        # aggregate review stats per cafe
        review_stats = self.reviews_df.groupby('place_id').agg({
            'rating' : ['mean', 'std', 'count'],
            'text_length' : 'mean',
            'word_count' : 'mean',
        }).count(2)

        merged = self.cafes_df.merge(
            review_stats,
            on='place_id',
            how='left'
        )

        return merged
    
    def generate_summary_stats(self) -> dict:
        """generate summary stats"""

        stats = {
            'total_cafes': len(self.cafes_df),
            'total_reviews': len(self.reviews_df),
            'cafes_by_region': self.cafes_df['region'].value_counts().to_dict(),
            'cafes_by_neighborhood': self.cafes_df['neighborhood'].value_counts().to_dict(),
            'avg_reviews_per_cafe': len(self.reviews_df) / len(self.cafes_df),
            'missing_reviews': self.cafes_df[~self.cafes_df['place_id'].isin(self.reviews_df['place_id'])].shape[0]
        }

        return stats



        