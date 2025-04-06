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
                self.process_denominator(node.right)
            else:
                self.generic_visit(node)  # Continue visiting child nodes

        def process_denominator(self, node):
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
                # Split addition into terms
                self.process_denominator(node.left)
                self.process_denominator(node.right)
            else:
                # Extract individual denominator components
                denominators = self.extract_variable_name(ast.unparse(node).strip())
                self.denominators.extend(denominators)

        def extract_variable_name(self, name):
            if '.fillna' in name:
                variables = []
                # Corrected logic here
                for var in name.split('.fillna'):
                    cleaned_var = var.replace('(', '').replace(')', '').strip()
                    if cleaned_var not in ('', '0'):
                        variables.append(cleaned_var)
                return variables
            # Extract the base variable name (ignore method calls)
            return [re.sub(r'\..*', '', name)]

    tree = ast.parse(expr, mode='eval')
    visitor = BinOpVisitor()
    visitor.visit(tree)
    return visitor.numerator, visitor.denominators
