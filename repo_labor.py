#SPDX-License-Identifier: MIT
"""
Metrics that provide data about labor investment
"""

import datetime
import sqlalchemy as s
import pandas as pd
from augur.util import register_metric

@register_metric()
def repo_labor(self, repo_group_id=1, repo_id=1, period='day', begin_date=None, end_date=None):

    config = {
    "connection_string": "sqlite:///:memory:",
    "database": "augur_osshealth",
    "host": "augur.osshealth.io",
    "password": "covfefe2020",
    "port": 5432,
    "schema": "augur_data",
    "user": "chaoss",
    "user_type": "read_only"
    }
            
    database_connection_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(config['user'], config['password'], config['host'], config['port'], config['database'])
    dbschema='augur_data'
    engine = s.create_engine(
    database_connection_string,
    connect_args={'options': '-csearch_path={}'.format(dbschema)})

    laborNewSQL = s.sql.text("""
            SELECT C.repo_id,
            C.repo_name,
            programming_language,
            SUM ( estimated_labor_hours ) AS labor_hours,
            SUM ( estimated_labor_hours * 50 ) AS labor_cost,
            analysis_date
            FROM
            (
            SELECT A
            .repo_id,
            b.repo_name,
            programming_language,
            SUM ( total_lines ) AS repo_total_lines,
            SUM ( code_lines ) AS repo_code_lines,
            SUM ( comment_lines ) AS repo_comment_lines,
            SUM ( blank_lines ) AS repo_blank_lines,
            AVG ( code_complexity ) AS repo_lang_avg_code_complexity,
            AVG ( code_complexity ) * SUM ( code_lines ) + 20 AS estimated_labor_hours,
            MAX ( A.rl_analysis_date ) AS analysis_date
            FROM
            repo_labor A,
            repo b
            WHERE
            A.repo_id = b.repo_id
            GROUP BY
            A.repo_id,
            programming_language,
            repo_name
            ORDER BY
            repo_name,
            A.repo_id,
            programming_language
            ) C
            GROUP BY
            repo_id,
            repo_name,
            programming_language,
            C.analysis_date
            ORDER BY
            repo_id,
            programming_language;
            """)

    results = pd.read_sql(laborNewSQL, engine, params={'repo_id': 1, 'repo_group_id': 1})
    return results
