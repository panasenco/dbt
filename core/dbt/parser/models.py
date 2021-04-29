from dbt.context.context_config import ContextConfig
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.dataclass_schema import ValidationError
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
        try:
            self.render_with_context(node, config)
            self.update_parsed_node(node, config)
        except ValidationError as exc:
            # we got a ValidationError - probably bad types in config()
            msg = validator_error_message(exc)
            raise CompilationException(msg, node=node) from exc