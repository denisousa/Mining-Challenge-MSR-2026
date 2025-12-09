import os
import ast
import argparse

class CodeSanitizer(ast.NodeTransformer):
    """
    Removes Type Hints and converts Async/Await syntax to synchronous syntax
    to ensure compatibility with older parsers (like NiCad/TXL).
    """
    
    def visit_FunctionDef(self, node):
        # Remove return annotation (e.g., -> str)
        node.returns = None
        # Remove decorators (optional, uncomment if decorators cause issues)
        # node.decorator_list = [] 
        self.generic_visit(node)
        return node

    def visit_AsyncFunctionDef(self, node):
        # Convert 'async def' to standard 'def'
        node.returns = None
        new_node = ast.FunctionDef(
            name=node.name,
            args=node.args,
            body=node.body,
            decorator_list=node.decorator_list,
            lineno=node.lineno
        )
        self.generic_visit(new_node)
        return new_node

    def visit_arg(self, node):
        # Remove argument annotations (e.g., x: int)
        node.annotation = None
        return node

    def visit_AnnAssign(self, node):
        # Convert 'x: int = 1' to 'x = 1'
        if node.value is None:
            # If it is just 'x: int' without a value, remove the line
            return None
        return ast.Assign(targets=[node.target], value=node.value, lineno=node.lineno)

    def visit_Await(self, node):
        # Remove 'await' keyword, keeping only the call (await foo() -> foo())
        return node.value

    def visit_AsyncFor(self, node):
        # Convert 'async for' to standard 'for'
        new_node = ast.For(
            target=node.target,
            iter=node.iter,
            body=node.body,
            orelse=node.orelse,
            lineno=node.lineno
        )
        self.generic_visit(new_node)
        return new_node

    def visit_AsyncWith(self, node):
        # Convert 'async with' to standard 'with'
        new_node = ast.With(
            items=node.items,
            body=node.body,
            lineno=node.lineno
        )
        self.generic_visit(new_node)
        return new_node

def clean_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            source = f.read()
        
        # Parse code to AST
        tree = ast.parse(source)
        
        # Apply sanitization
        sanitizer = CodeSanitizer()
        tree = sanitizer.visit(tree)
        ast.fix_missing_locations(tree)
        
        # Generate clean code (Requires Python 3.9+)
        clean_source = ast.unparse(tree)
        
        # Overwrite the file
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(clean_source)
            
        print(f"[OK] Cleaned: {filepath}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to clean {filepath}: {e}")
        return False

def process_directory_py(directory):
    count = 0
    errors = 0
    print(f"Starting cleaning process in: {directory}")
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                path = os.path.join(root, file)
                if clean_file(path):
                    count += 1
                else:
                    errors += 1
    print(f"\nDone. Files cleaned: {count}, Errors: {errors}")