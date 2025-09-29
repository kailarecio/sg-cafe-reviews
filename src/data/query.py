from datetime import datetime
import googlemaps
import os
import pandas as pd
import time
import yaml

class CafeQuery:
    def __init__(self, config_path = 'config/neighborhoods.yaml'):
        """Initialize with API client and config"""

        # remove later when tests are live
        from dotenv import load_dotenv
        load_dotenv()

        # initialize client
        self.gmaps = googlemaps.Client(key=os.getenv('GOOGLE_MAPS_API_KEY'))

        # load in yaml file
        with open('config/neighborhoods.yaml', 'r') as file:
            config = yaml.safe_load(file)

        # flatten yaml file's contents
        self.neighborhoods = []

        for region, neighborhood in config.items():
            for name, details in neighborhood.items():
                details['neighborhood_name'] = name
                details['region'] = region
                self.neighborhoods.append(details)

        print(f'Client initialized and cafe reviews for {len(self.neighborhoods)} neighborhoods will be extracted')

    def collect_cafes(self, neighborhood: dict) -> pd.DataFrame:
        """Collect all cafe metadata from one neighborhood"""

        all_cafes = []

        response = self.gmaps.places_nearby(
            location=tuple(neighborhood['center']), # long, lat coordinates
            radius=neighborhood['radius'],
            type='cafe',
            language='en'
        )

        all_cafes.extend(response.get('results', []))

        # API only returns max 20 results per request
        # use next_page_token to get the next 20 (max 60)
        while 'next_page_token' in response:
            time.sleep(2)  # Google requires 2-second delay
            response = self.gmaps.places_nearby(
                page_token=response['next_page_token']
            )
            all_cafes.extend(response['results'])

        # convert to df
        if all_cafes:
            df = pd.DataFrame(all_cafes)

            # keep only necessary columns
            keep_cols = ['name', 'place_id', 'rating', 'user_ratings_total', 'price_level', 'types', 'business_status', 'permanently_closed']
            df = df[[col for col in keep_cols if col in df.columns]]

            # append with neighborhood and region
            df['neighborhood'] = neighborhood['neighborhood_name']
            df['region'] = neighborhood['region']

            return df
        
        return pd.DataFrame()
    
    def collect_all(self) -> pd.DataFrame:
        """Apply function above to collect cafe metadata from all neighborhoods"""
        dfs = []
        
        for i, neighborhood in enumerate(self.neighborhoods, 1):
            print(f"[{i}/{len(self.neighborhoods)}] Collecting {neighborhood['name']}...")
            df = self.collect_neighborhood(neighborhood)
            dfs.append(df)
            print(f"  Found {len(df)} cafes")
            time.sleep(1) 

        return pd.concat(dfs, ignore_index=True)
    
    def collect_reviews(self, place_ids: list, batch_size: int = 10) -> pd.DataFrame:
        """Collect reviews for a list of cafe place_ids"""
        all_reviews = []

        for i, place_id in enumerate(place_ids):
            try:
                # Get place_id, name and reviews
                response = self.gmaps.place(
                    place_id=place_id,
                    fields=['name', 'reviews'],
                    language='en'
                )
                
                place_name = response['result'].get('name', 'Unknown')
                reviews = response['result'].get('reviews', [])

                # Process each review
                for review in reviews:
                    # Convert Unix timestamp to date
                    review_date = pd.to_datetime(review.get('time'), unit='s').date() if review.get('time') else None
                    
                    # Extract author ID from URL (the numbers at the end)
                    author_url = review.get('author_url', '')
                    author_id = author_url.split('/')[-1] if author_url else None
                    
                    review_data = {
                        'place_id': place_id,
                        'place_name': place_name,
                        'author_id': author_id,  # Unique identifier from URL
                        'rating': review.get('rating'),
                        'text': review.get('text', ''),
                        'review_date': review_date,
                        'relative_time_description': review.get('relative_time_description')
                    }
                    all_reviews.append(review_data)
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error getting reviews for {place_name}: {e}")
                continue
        
        return pd.DataFrame(all_reviews)