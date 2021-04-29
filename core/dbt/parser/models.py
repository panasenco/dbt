from dbt.context.context_config import ContextConfig
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.dataclass_schema import ValidationError
from dbt.dbt_jinja.compiler import extract_from_source
from dbt.exceptions import CompilationException
from dbt.node_types import NodeType
from dbt.parser.base import IntermediateNode, SimpleSQLParser
from dbt.parser.search import FileBlock


class ModelParser(SimpleSQLParser[ParsedModelNode]):
    def parse_from_dict(self, dct, validate=True) -> ParsedModelNode:
        if validate:
            ParsedModelNode.validate(dct)
        return ParsedModelNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Model

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def render_update(
        self, node: IntermediateNode, config: ContextConfig
    ) -> None:
        # run dbt-jinja extractor (powered by tree-sitter)
        res = extract_from_source(node.raw_sql)
        # if it didn't return an exception, fit the refs, sources, and configs
        # into the node. Down the line the rest of the node will be updated with
        # this information. (e.g. depends_on etc.)
        if not isinstance(res, Exception):
            for ref in res['refs']:
                node.refs.append(ref)
            for source in res['sources']:
                # TODO change extractor to match type here
                node.sources.append([source[0], source[1]])
            for config in res['configs']:
                node.config[config[0]] = config[1]

            # TODO is this line necessary? not even sure what it does.
            self.update_parsed_node(node, config)
        else: 
            super().render_update(node, config)
