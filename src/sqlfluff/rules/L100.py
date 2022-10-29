from inspect import getmembers
from pprint import pprint

"""Implementation of Rule L100."""
from typing import Optional

from sqlfluff.utils.analysis.select_crawler import Query, SelectCrawler
from sqlfluff.core.parser import BaseSegment
from sqlfluff.core.rules import BaseRule, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.core.rules.doc_decorators import document_groups
from sqlfluff.utils.functional import sp, FunctionalContext, Segments


_START_TYPES = ["select_statement", "set_expression", "with_compound_statement"]


class RuleFailure(Exception):
    """Exception class for reporting lint failure inside deeply nested code."""

    def __init__(self, anchor: BaseSegment):
        self.anchor: BaseSegment = anchor


@document_groups
class Rule_L100(BaseRule):
    """Query produces an unknown number of result columns.

    **Anti-pattern**

    Querying all columns using ``*`` produces a query result where the number
    or ordering of columns changes if the upstream table's schema changes.
    This should generally be avoided because it can cause slow performance,
    cause important schema changes to go undetected, or break production code.
    For example:

    * If a query does ``SELECT t.*`` and is expected to return columns ``a``, ``b``,
      and ``c``, the actual columns returned will be wrong/different if columns
      are added to or deleted from the input table.
    * ``UNION`` and ``DIFFERENCE`` clauses require the inputs have the same number
      of columns (and compatible types).
    * ``JOIN`` queries may break due to new column name conflicts, e.g. the
      query references a column ``c`` which initially existed in only one input
      table but a column of the same name is added to another table.
    * ``CREATE TABLE (<<column schema>>) AS SELECT *``


    .. code-block:: sql

        WITH cte AS (
            SELECT * FROM foo
        )

        SELECT * FROM cte
        UNION
        SELECT a, b FROM t

    **Best practice**

    Somewhere along the "path" to the source data, specify columns explicitly.

    .. code-block:: sql

        WITH cte AS (
            SELECT * FROM foo
        )

        SELECT a, b FROM cte
        UNION
        SELECT a, b FROM t

    """

    groups = ("all",)
    crawl_behaviour = SegmentSeekerCrawler(set(_START_TYPES))

    def _validate_reference_qualified(self, table_reference_segment):
        
        if not table_reference_segment.is_qualified():
            self.logger.debug(
                f"Table Reference {table_reference_segment.raw} is not qualified"
            )
            return LintResult(
                anchor=table_reference_segment,
                description=f"Table Reference '{table_reference_segment.raw}' is not qualified.",
            )

    def _analyze_query(self, query: Query):
        """Analyse query to check if immediate sources fail"""
        
        # Recursively walk from the given query to any table_reference segments.
        
        self.logger.debug(query.as_json())
        
        if not query.selectables:
            return  # pragma: no cover
        violations = []
        for selectable in query.selectables:
            self.logger.debug(f"Analyzing query: {selectable.selectable.raw}")
            
            for source in query.crawl_sources(selectable.selectable, pop=True, return_segment=True):
                if isinstance(source, Query):
                    self.logger.debug(f"source is a Query: {source.as_json()}")
                    # source is a cte or subquery. Recurse into analysis
                    self._analyze_query(source)
                else:
                    # source is an external table reference.
                    # check it is qualified
                    self.logger.debug(f"source is table_ref: {source.raw}")
                    violations.append(self._validate_reference_qualified(source))

        return violations
            
            
        #for from_expression_element in selectable.select_info.from_expression_elements:
        #    print('------------------- FEE:')
        #    print(from_expression_element.raw)
        #    print('------------------------')
            
            # Is it another query?
        '''

        print('------------------- bool:')
        print(f"thing: {type(from_expression_element)}")
        print('------------------------')

        

        cte = query.lookup_cte(wildcard_table)
        if cte:
            self.logger.debug(
                f"CTE: {cte.as_json()}"
            )
            # Wildcard refers to a CTE. Analyze it.
            self._analyze_query(cte)
        else:
            # Not CTE, not table alias. Presumably an
            # external table. Warn.
            self.logger.debug(
                f"Query target {wildcard_table} is external. "
                "Generating warning."
            )
            raise RuleFailure(selectable.selectable)
        '''
        '''
        if wildcard.tables:
            for wildcard_table in wildcard.tables:
                self.logger.debug(
                    f"Wildcard: {wildcard.segment.raw} has target {wildcard_table}"
                )
                # Is it an alias?
                alias_info = selectable.find_alias(wildcard_table)

                if alias_info:
                    # Found the alias matching the wildcard. Recurse,
                    # analyzing the query associated with that alias.
                    self._handle_alias(selectable, alias_info, query)
                else:
                    # Not an alias. Is it a CTE?
                    cte = query.lookup_cte(wildcard_table)
                    if cte:
                        self.logger.debug(
                            f"CTE: {cte.as_json()}"
                        )
                        # Wildcard refers to a CTE. Analyze it.
                        self._analyze_query(cte)
                    else:
                        # Not CTE, not table alias. Presumably an
                        # external table. Warn.
                        self.logger.debug(
                            f"Query target {wildcard_table} is external. "
                            "Generating warning."
                        )
                        raise RuleFailure(selectable.selectable)
        else:
            # No table was specified with the wildcard. Assume we're
            # querying from a nested select in FROM.
            query_list = SelectCrawler.get(
                query, query.selectables[0].selectable
            )
            for o in query_list:
                if isinstance(o, Query):
                    self._analyze_query(o)
                    return
            self.logger.debug(
                f'Query target "{query.selectables[0].selectable.raw}" has no '
                "targets. Generating warning."
            )
            raise RuleFailure(query.selectables[0].selectable)
        '''

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Outermost query should produce known number of columns."""
        if not FunctionalContext(context).parent_stack.any(sp.is_type(*_START_TYPES)):
            crawler = SelectCrawler(context.segment, context.dialect)

            # Begin analysis at the outer query.
            if crawler.query_tree:
                try:
                    return self._analyze_query(crawler.query_tree)
                except RuleFailure as e:
                    return LintResult(anchor=e.anchor)
        return None
