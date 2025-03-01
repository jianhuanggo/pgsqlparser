import re
from typing import Dict, List, Set
from dataclasses import dataclass
from sqlglot import exp, parse_one
from sqlglot.dialects import Redshift, Databricks


@dataclass
class CTENode:
    """Represents a CTE node in the SQL graph."""
    name: str
    sql: exp.Expression
    references: Set[str]
    referenced_by: Set[str]
    aliases: Dict[str, str]


class SQLTranslator:
    def __init__(self):
        self.cte_counter = 0
        self.cte_graph: Dict[str, CTENode] = {}

    def _generate_cte_name(self) -> str:
        """Generate unique CTE names."""
        self.cte_counter += 1
        return f"cte_alias_{self.cte_counter}"

    def _build_cte_graph(self, sql_exp: exp.Expression) -> None:
        """Build a graph of CTE dependencies."""
        if isinstance(sql_exp, exp.With):
            for cte in sql_exp.expressions:
                cte_name = str(cte.alias)
                cte_sql = cte.expression
                references = set()
                aliases = {}

                # Find CTE references and column aliases
                for node in cte_sql.walk():
                    if (isinstance(node, exp.Table) and
                            str(node) in self.cte_graph):
                        references.add(str(node))
                    elif isinstance(node, exp.Alias):
                        aliases[str(node.alias)] = str(node.expression)

                self.cte_graph[cte_name] = CTENode(
                    name=cte_name,
                    sql=cte_sql,
                    references=references,
                    referenced_by=set(),
                    aliases=aliases
                )

            # Update referenced_by relationships
            for node in self.cte_graph.values():
                for ref in node.references:
                    if ref in self.cte_graph:
                        self.cte_graph[ref].referenced_by.add(node.name)

        # Process nested queries
        for child in sql_exp.walk():
            if isinstance(child, exp.Select):
                self._build_cte_graph(child)

    def _handle_column_aliases(
        self, sql_exp: exp.Expression
    ) -> exp.Expression:
        """Process column aliases referenced in same SELECT statement."""

        if not isinstance(sql_exp, exp.Select):
            return sql_exp

        # Find column aliases and their references
        aliases = {}
        alias_references = set()

        for node in sql_exp.walk():
            if isinstance(node, exp.Alias):
                aliases[str(node.alias)] = node
            elif isinstance(node, exp.Column) and str(node) in aliases:
                alias_references.add(str(node))

        if alias_references:
            # Create a new CTE for this SELECT
            cte_name = self._generate_cte_name()
            # Create the CTE node
            cte_node = CTENode(
                name=cte_name,
                sql=sql_exp,
                references=set(),
                referenced_by=set(),
                aliases=aliases
            )
            self.cte_graph[cte_name] = cte_node

            # Return a new SELECT that references the CTE
            return exp.select("*").from_(exp.Table(cte_name))

        return sql_exp

    def _handle_ignore_nulls(self, sql: str) -> str:
        """Replace IGNORE NULLS with Databricks equivalent CASE statements."""
        pattern = (
            r'(FIRST_VALUE|'
            r'LAST_VALUE)'
            r'\((.*?)\)'
            r'\s+IGNORE\s+NULLS\s+OVER\s*\((.*?)\)'
        )

        def replace_ignore_nulls(match):
            func = match.group(1)
            expr = match.group(2)
            window = match.group(3)
            return (
                f"{func}(CASE WHEN {expr} IS NOT NULL THEN {expr} END) "
                f"OVER ({window})"
            )

        # Handle both FIRST_VALUE and LAST_VALUE
        sql = re.sub(
            pattern,
            replace_ignore_nulls,
            sql,
            flags=re.IGNORECASE | re.DOTALL
        )

        # Handle other window functions with IGNORE NULLS
        pattern = (
            r'([A-Za-z_][A-Za-z0-9_]*)\((.*?)\)\s+'
            r'IGNORE\s+NULLS\s+OVER\s*\((.*?)\)'
        )
        return re.sub(
            pattern, replace_ignore_nulls, sql, flags=re.IGNORECASE | re.DOTALL
        )

    def _handle_string_concat(self, sql: str) -> str:
        """Replace string concatenation using + with concat()."""
        def concat_handler(match):
            parts = []
            current_pos = 0
            text = match.group(0)

            while current_pos < len(text):
                if text[current_pos] == "'":
                    end_quote = text.find("'", current_pos + 1)
                    if end_quote != -1:
                        parts.append(text[current_pos:end_quote + 1])
                        current_pos = end_quote + 1
                    else:
                        parts.append(text[current_pos:])
                        break
                elif text[current_pos] == '+':
                    parts.append(',')
                else:
                    parts.append(text[current_pos])
                current_pos += 1

            return f"concat({(''.join(parts)).strip()})"

        pattern = (
            r"(?:[a-zA-Z_][a-zA-Z0-9_]*|'[^']*')\s*"
            r"(?:\+\s*(?:[a-zA-Z_][a-zA-Z0-9_]*|'[^']*'))+?"
        )
        return re.sub(pattern, concat_handler, sql)

    def _handle_regexp_functions(self, sql: str) -> str:
        """Convert regexp functions to Databricks format."""
        def regexp_substr_handler(match):
            expr, pattern = match.group(1), match.group(2)
            pos = match.group(3) if match.group(3) else "1"

            # Handle lookbehind patterns specially
            if "?<=" in pattern:
                # Convert lookbehind pattern to a more compatible form
                cte_name = self._generate_cte_name()
                pattern = (
                    pattern.replace("(?<=\\()", "\\(")
                    .replace("(?<=", "")
                )
                cte_query = (
                    f"(WITH {cte_name} AS "
                    f"(SELECT regexp_extract({expr}, {pattern}, "
                    f"{int(pos)-1}) as match) "
                    f"SELECT match FROM {cte_name})"
                )
                return cte_query
            return f"regexp_extract({expr}, {pattern}, {int(pos)-1})"

        sql = re.sub(
            r'regexp_substr\((.*?),\s*([^,]+)(?:,\s*(\d+)(?:,\s*(\d+))?)?\)',
            regexp_substr_handler,
            sql,
            flags=re.IGNORECASE
        )

        # Replace regexp_instr with a combination of regexp_extract and length
        regexp_instr_repl = (
            r'(WITH regexp_match AS '
            r'(SELECT regexp_extract(\1, \2, 0) as match) '
            r'SELECT CASE WHEN match IS NOT NULL '
            r'THEN length(match) ELSE 0 END FROM regexp_match)'
        )
        sql = re.sub(
            r'regexp_instr\((.*?),\s*(.*?)(?:,\s*(\d+))?\)',
            regexp_instr_repl,
            sql,
            flags=re.IGNORECASE
        )

        # Handle regexp_count
        regexp_count_repl = (
            r'(WITH regexp_matches AS '
            r'(SELECT regexp_extract_all(\1, \2) as matches) '
            r'SELECT size(matches) FROM regexp_matches)'
        )
        sql = re.sub(
            r'regexp_count\((.*?),\s*(.*?)(?:,\s*(\d+))?\)',
            regexp_count_repl,
            sql,
            flags=re.IGNORECASE
        )
        return sql

    def _apply_transformations(
        self, sql_exp: exp.Expression
    ) -> exp.Expression:
        """Apply all SQL transformations to an expression."""

        sql_exp = self._handle_column_aliases(sql_exp)
        sql_exp = self._handle_ignore_nulls(sql_exp)
        sql_exp = self._handle_string_concat(sql_exp)
        sql_exp = self._handle_regexp_functions(sql_exp)
        return sql_exp

    def _sort_ctes_topologically(self) -> List[str]:
        """Sort CTEs in topological order based on dependencies."""
        visited = set()
        sorted_ctes = []

        def visit(cte_name: str) -> None:
            if cte_name in visited:
                return
            visited.add(cte_name)
            for ref in self.cte_graph[cte_name].references:
                if ref in self.cte_graph:
                    visit(ref)
            sorted_ctes.append(cte_name)

        for cte_name in self.cte_graph:
            visit(cte_name)
        return sorted_ctes

    def translate(self, sql: str) -> str:
        """Translate Redshift SQL to Databricks SQL."""
        # Parse SQL using sqlglot
        sql_exp = parse_one(sql, read=Redshift)
        # Build CTE graph
        self._build_cte_graph(sql_exp)
        # Apply transformations
        sql_exp = self._apply_transformations(sql_exp)
        # Sort CTEs topologically
        sorted_ctes = self._sort_ctes_topologically()

        # Reconstruct SQL with ordered CTEs
        if sorted_ctes:
            cte_expressions = []
            for cte_name in sorted_ctes:
                node = self.cte_graph[cte_name]
                cte_expressions.append(
                    exp.CTE(
                        this=node.sql,
                        alias=exp.TableAlias(this=exp.to_identifier(cte_name))
                    )
                )
            sql_exp = exp.With(
                expressions=cte_expressions,
                expression=sql_exp
            )
        # Convert to Databricks dialect
        return sql_exp.sql(dialect=Databricks)
