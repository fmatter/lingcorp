import logging
import re
from dataclasses import dataclass

from parsimonious.exceptions import ParseError
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor

log = logging.getLogger(__name__)

# attr_pattern = r'(?P<attr>.*?)\s?=\s?"(?P<value>.*?)"'
# def parse_token(token):
#     args = {x.group("attr"): x.group("value") for x in re.finditer(attr_pattern, token)}
#     return args

# token_pattern = re.compile(r"\[\s?(?P<arglist>.*?)\s?\]")
# def parse(query_string):
#     query_string = query_string.replace("'", '"')
#     query_list = []
#     for token in token_pattern.finditer(query_string):
#         query_list.append(parse_token(token.group("arglist")))
#     print(query_list)


@dataclass
class AttrValue:
    attr: str
    comparator: str
    val: str

    def __repr__(self):
        return f'{self.attr}{self.comparator}"{self.val}"'


@dataclass
class BaseExpression:
    def match(self, dict):
        log.warning("UNIMPLEMENTED MATCH FUNCTION")
        return False


@dataclass
class Expression(BaseExpression):
    attr_val: AttrValue = None

    def match(self, dic):
        def _match(pattern, value, regex=True, pos=True):
            if isinstance(value, list):
                if pos:
                    if regex:
                        if any(pattern.match(value) for value in value):
                            return True
                    elif any(pattern == value for value in value):
                        return True
                else:
                    if regex:
                        if not any(pattern.match(value) for value in value):
                            return True
                    elif not any(pattern == value for value in value):
                        return True
            else:
                if pos:
                    if regex:
                        if pattern.match(value):
                            return True
                    elif pattern == value:
                        return True
                else:
                    if regex:
                        if not pattern.match(value):
                            return True
                    elif pattern != value:
                        return True
            return False

        if not self.attr_val:
            return True
        value = dic.get(self.attr_val.attr, "")
        if self.attr_val.comparator == "=":
            pattern = re.compile("^" + self.attr_val.val.replace("*", ".*?") + "$")
            return _match(pattern, value)
        if self.attr_val.comparator == "!=":
            pattern = re.compile("^" + self.attr_val.val.replace("*", ".*?") + "$")
            return _match(pattern, value, pos=False)
        if self.attr_val.comparator == "==":
            return _match(self.attr_val.val, value, regex=False)
        if self.attr_val.comparator == "!==":
            return _match(self.attr_val.val, value, pos=False, regex=False)

    def __repr__(self):
        return str(self.attr_val)


@dataclass
class GroupExpression(BaseExpression):
    a: Expression
    b: Expression


@dataclass
class And(GroupExpression):
    def match(self, dict):
        return self.a.match(dict) and self.b.match(dict)

    def __repr__(self):
        return f"({self.a} & {self.b})"


@dataclass
class Or(GroupExpression):
    def match(self, dict):
        return self.a.match(dict) or self.b.match(dict)

    def __repr__(self):
        return f"({self.a} | {self.b})"


@dataclass
class Token:
    expr: BaseExpression

    def match(self, dict):
        return self.expr.match(dict)

    def __repr__(self):
        return f"[{self.expr}]"


class SCLVisitor(NodeVisitor):
    # def __init__(self):
    #     self.tokens = []
    # tree = parse('[lemma = "ref*" & tag=="imp"] [ ] [lemma="kettle"]')

    def visit_query(self, node, v_c):
        return v_c

    def visit_token(self, node, v_c):
        expr = v_c[1]
        if isinstance(expr, list):  # non-empty token
            return Token(v_c[1][0])
        else:  # empty token
            return Token(Expression())
        return v_c or node

    def visit_expression(self, node, v_c):
        # print("\nEXPRESSION", node.text, len(v_c[0]))
        children = v_c[0]
        if len(children) == 2:  # no parentheses
            if isinstance(children[1], list):  # complex expression
                expr1 = Expression(children[0])
                boolean = children[1][0][0][0]
                expr2 = children[1][0][1]
                return boolean(a=expr1, b=expr2)
            else:
                return Expression(children[0])  # simple expression
        else:  # parentheses
            return children[1]
        return v_c or node

    def visit_AND(self, n, v_c):
        return And

    def visit_OR(self, n, v_c):
        return Or

    def visit_attr_val(self, node, v_c):
        key, comparator, _, value, _ = node.children
        # print(AttrValue(key.text, comparator.text, value.text))
        return AttrValue(key.text, comparator.text, value.text)

        # print(key, ":")
        # print(values)

    def generic_visit(self, node, v_c):
        """The generic visit method."""
        return v_c or node


def strip_whitespace(txt):
    return '"'.join(
        it if i % 2 else "".join(it.split()) for i, it in enumerate(txt.split('"'))
    )


def strip_comments(s):
    return "\n".join([x for x in s.split("\n") if not x.strip().startswith("#")])


grammar = Grammar(
    strip_comments(
        r"""query = token+
token = token_open expression? token_close
token_open  = "["
token_close = "]"
expression =  (attr_val (bool expression)?) / (lpar expression rpar) 
lpar = "("
rpar = ")"
bool = AND / OR
AND = "&"
OR = "|"
attr_val = attr comparator quote? val quote?
quote = '"'
attr = anything
val = anything
anything = ~"[a-z0-9\?\u0080-\uFFFF\*\-]+"i
comparator = equal_plain / equal  / not_equal_plain / not_equal
equal = "="
equal_plain = "=="
not_equal = "!="
not_equal_plain = "!=="
"""
    )
)


def parse(query_string):
    query_string = query_string.replace("'", '"')
    query_string = strip_whitespace(query_string)
    try:
        tree = grammar.parse(query_string)
        visitor = SCLVisitor()
        return visitor.visit(tree)
    except ParseError as e:
        log.warning(e)
        log.warning(f"Invalid query: '{query_string}'")
        return None
