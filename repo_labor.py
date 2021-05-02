#SPDX-License-Identifier: MIT
"""
Metrics that provide data about commits & their associated activity
"""

import datetime
import sqlalchemy as s
import pandas as pd
from augur.util import register_metric

@register_metric()
def get_labor(self, repo_id=None, repo_name, programming_language):

    if not repo_name:
        repo_name = 'repo_labor'
    
    laborNewSQlGetParams = s.sql.text("""

            

    """
    )

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
    results = pd.read_sql(laborNewSQL, self.database, 
            params={'repo_id': repo_id, 'programming_language': "Python"})
    results = ["WE DID IT"]
    return results
