import ast


def extract_numerator_denominator(expr):
    """
    Parse a string expression and extract the numerator and denominator of the first division.

    Args:
        expression (str): The mathematical expression to parse.

    Returns:
        tuple: A tuple containing the numerator and denominator as strings, or None if no division is found.
    """

    class BinOpVisitor(ast.NodeVisitor):
        def __init__(self):
            self.result = None

        def visit_BinOp(self, node):
            # Check if the operator is division and if result isn't already set
            if isinstance(node.op, ast.Div) and self.result is None:
                # Extract the numerator (left) and denominator (right)
                numerator = ast.unparse(node.left).strip()
                denominator = ast.unparse(node.right).strip()
                self.result = (numerator, denominator)
                return  # Stop visiting further nodes
            # Continue visiting child nodes
            self.generic_visit(node)

    # Parse the expression into an AST
    tree = ast.parse(expr, mode='eval')
    visitor = BinOpVisitor()
    visitor.visit(tree)
    return visitor.result
