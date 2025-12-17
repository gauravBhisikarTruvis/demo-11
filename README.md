import sqlglot
from sqlglot import exp
from typing import Dict, List, Set
import logging

logger = logging.getLogger(__name__)


class BigQueryTableColumnExtractor:
    """
    Extracts tables and referenced columns from BigQuery SQL.
    Input: SQL only
    Output: {table_identifier: [columns]}
    """

    def extract(self, sql: str) -> Dict[str, List[str]]:
        parsed = sqlglot.parse(sql, read="bigquery")
        if not parsed:
            return {}

        table_columns: Dict[str, Set[str]] = {}

        for statement in parsed:
            self._process_statement(statement, table_columns)

        return {
            table: sorted(columns)
            for table, columns in table_columns.items()
        }

    def _process_statement(
        self,
        statement: exp.Expression,
        table_columns: Dict[str, Set[str]],
    ):
        alias_to_table: Dict[str, str] = {}

        # ---------- Pass 1: collect tables ----------
        for table in statement.find_all(exp.Table):
            table_name = self._table_identifier(table)
            table_columns.setdefault(table_name, set())

            alias = table.alias
            if alias:
                alias_to_table[alias] = table_name

        # ---------- Pass 2: collect columns ----------
        for column in statement.find_all(exp.Column):
            column_name = column.name
            table_ref = column.table

            if table_ref:
                resolved_table = alias_to_table.get(table_ref)
                if resolved_table:
                    table_columns[resolved_table].add(column_name)
            else:
                # unqualified column: assign only if single table
                if len(table_columns) == 1:
                    only_table = next(iter(table_columns))
                    table_columns[only_table].add(column_name)

    def _table_identifier(self, table: exp.Table) -> str:
        """
        Returns table identifier exactly as written:
        - project.dataset.table
        - dataset.table
        - table
        """
        return ".".join(part.name for part in table.parts)


def extract_table_columns_from_query(sql_query: str) -> Dict[str, List[str]]:
    extractor = BigQueryTableColumnExtractor()
    return extractor.extract(sql_query)
