"""Implementation of Rule L100."""
from typing import Optional

from sqlfluff.utils.analysis.select_crawler import Query, SelectCrawler
from sqlfluff.core.parser import BaseSegment
from sqlfluff.core.rules import BaseRule, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler
from sqlfluff.core.rules.doc_decorators import document_groups
from sqlfluff.utils.functional import sp, FunctionalContext, Segments


_START_TYPES = [
    "select_statement",
    "set_expression",
    "with_compound_statement",
]

@document_groups
class Rule_L100(BaseRule):
    """Enforce schemas to be specified in table references

    **Anti-pattern**

    In some databases, it may be the case that people want
    to enforce the schema being specified each time a table
    or view is reference rather than just leaving it as implied.

    .. code-block:: sql

        SELECT
            column
        FROM
            table

    **Best practice**

    Specify a specific schema

    .. code-block:: sql

        SELECT
            column
        FROM
            public.table

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
        
        if not query.selectables:
            return  # pragma: no cover
        
        violations: List[LintResult] = []

        for selectable in query.selectables:
            self.logger.debug(f"Analyzing query: {selectable.selectable.raw}")
            
            for source in query.crawl_sources(selectable.selectable, pop=True, return_segment=True):
                if isinstance(source, Query):
                    # source is a cte or subquery. Recurse into analysis
                    self._analyze_query(source)
                else:
                    # source is an external table reference.
                    # check it is qualified
                    violations.append(self._validate_reference_qualified(source))

        return violations

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Recursively check any ctes"""
        if not FunctionalContext(context).parent_stack.any(sp.is_type(*_START_TYPES)):
            crawler = SelectCrawler(context.segment, context.dialect)

            # Begin analysis at the outer query.
            if crawler.query_tree:
                return self._analyze_query(crawler.query_tree)
        return None
