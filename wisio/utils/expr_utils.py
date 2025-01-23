import ast
import re


def extract_numerator_and_denominators(expr):
    class BinOpVisitor(ast.NodeVisitor):
        def __init__(self):
            self.numerator = None
            self.denominators = []

        def visit_BinOp(self, node):
            if isinstance(node.op, ast.Div) and self.numerator is None:
                self.numerator = ast.unparse(node.left).strip()
                if isinstance(node.right, ast.BinOp) and isinstance(
                    node.right.op, ast.Add
                ):
                    self.visit_Add(node.right)
                elif '/' in ast.unparse(node.right):
                    self.denominators.extend(
                        [
                            self.clean_variable_name(den.strip())
                            for den in ast.unparse(node.right).split('/')
                        ]
                    )
                else:
                    self.denominators.append(
                        self.clean_variable_name(ast.unparse(node.right).strip())
                    )
                return
            self.generic_visit(node)

        def visit_Add(self, node):
            if isinstance(node.left, ast.BinOp) and isinstance(node.left.op, ast.Add):
                self.visit_Add(node.left)
            else:
                self.denominators.append(
                    self.clean_variable_name(ast.unparse(node.left).strip())
                )
            self.denominators.append(
                self.clean_variable_name(ast.unparse(node.right).strip())
            )

        def clean_variable_name(self, name):
            return re.sub(r'\..*', '', name)

    tree = ast.parse(expr, mode='eval')
    visitor = BinOpVisitor()
    visitor.visit(tree)
    return visitor.numerator, visitor.denominators
