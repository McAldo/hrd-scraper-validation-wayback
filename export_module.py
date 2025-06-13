# file: export_module.py

import os
import logging
from typing import Dict, Any

import pandas as pd
from sqlalchemy.orm import Session

from db import Profile, URL   # ← Add this line

logger = logging.getLogger(__name__)

class Exporter:
    def __init__(self, db_session: Session, output_dir: str = "./output"):
        """
        Args:
            db_session (Session): Active SQLAlchemy session.
            output_dir (str): Directory to write CSVs.
        """
        self.db = db_session
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def export(self,
               include_profiles: bool = True,
               include_urls: bool = True,
               to_pandas: bool = False
               ) -> Dict[str, Any]:
        """
        Export tables to CSV and optionally return DataFrames.

        Args:
            include_profiles (bool): Export profiles table.
            include_urls (bool): Export urls table.
            to_pandas (bool): Return DataFrames if True.

        Returns:
            Dict[str, Any]: {
                'profiles_csv': path or None,
                'urls_csv': path or None,
                'profiles_df': DataFrame or None,
                'urls_df': DataFrame or None,
                'counts': {'profiles': n, 'urls': m}
            }
        """
        report: Dict[str, Any] = {
            'profiles_csv': None,
            'urls_csv': None,
            'profiles_df': None,
            'urls_df': None,
            'counts': {}
        }

        if include_profiles:
            df_profiles = pd.read_sql(self.db.query(Profile).statement,
                                      self.db.bind)
            path_p = os.path.join(self.output_dir, "profiles.csv")
            df_profiles.to_csv(path_p, index=False, encoding='utf-8')
            report['profiles_csv'] = path_p
            report['counts']['profiles'] = len(df_profiles)
            if to_pandas:
                report['profiles_df'] = df_profiles

        if include_urls:
            df_urls = pd.read_sql(self.db.query(URL).statement,
                                  self.db.bind)
            path_u = os.path.join(self.output_dir, "urls.csv")
            df_urls.to_csv(path_u, index=False, encoding='utf-8')
            report['urls_csv'] = path_u
            report['counts']['urls'] = len(df_urls)
            if to_pandas:
                report['urls_df'] = df_urls

        return report
