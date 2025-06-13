from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
import pandas as pd
from db import init_db, Profile, URL

def analyze_database():
    # Initialize database connection
    SessionLocal = init_db("sqlite:///hrd.db", echo=False)
    
    with SessionLocal() as session:
        # Get basic statistics
        total_profiles = session.query(Profile).count()
        total_urls = session.query(URL).count()
        
        print(f"\nDatabase Statistics:")
        print(f"Total Profiles: {total_profiles}")
        print(f"Total URLs: {total_urls}")
        
        # Analyze profiles
        print("\nProfile Analysis:")
        profiles_df = pd.read_sql(session.query(Profile).statement, session.bind)
        
        # Count non-null values for each column
        non_null_counts = profiles_df.count()
        print("\nNon-null values in Profiles table:")
        for column, count in non_null_counts.items():
            print(f"{column}: {count}")
        
        # Analyze URLs
        print("\nURL Analysis:")
        urls_df = pd.read_sql(session.query(URL).statement, session.bind)
        
        # Count URLs per profile
        urls_per_profile = urls_df.groupby('profile_id').size()
        print(f"\nURLs per profile statistics:")
        print(f"Min URLs per profile: {urls_per_profile.min()}")
        print(f"Max URLs per profile: {urls_per_profile.max()}")
        print(f"Average URLs per profile: {urls_per_profile.mean():.2f}")
        
        # Sample some profiles with their URLs
        print("\nSample Profile with URLs:")
        sample_profile = session.query(Profile).first()
        if sample_profile:
            print(f"\nProfile: {sample_profile.name}")
            print(f"Country: {sample_profile.country}")
            print(f"Region: {sample_profile.region}")
            print(f"Type of Work: {sample_profile.type_of_work}")
            print("\nAssociated URLs:")
            for url in sample_profile.urls:
                print(f"- {url.label}: {url.url}")
                print(f"  Active: {url.is_active}")
                print(f"  Archived: {url.is_archived}")
                if hasattr(url, 'contains_name'):
                    print(f"  Contains Name: {url.contains_name}")

if __name__ == "__main__":
    analyze_database() 