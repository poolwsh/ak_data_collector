import os
import ast
import pkg_resources

def get_imports_from_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        tree = ast.parse(file.read(), filename=file_path)
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module.split('.')[0])
    return imports

def get_installed_packages():
    return {pkg.key for pkg in pkg_resources.working_set}

def generate_requirements(dags_dir, output_file):
    imported_modules = set()
    for root, _, files in os.walk(dags_dir):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                imported_modules.update(get_imports_from_file(file_path))

    installed_packages = get_installed_packages()
    requirements = imported_modules.intersection(installed_packages)

    with open(output_file, "w") as f:
        for requirement in sorted(requirements):
            f.write(f"{requirement}\n")

if __name__ == "__main__":
    dags_dir = "./dags"
    output_file = "requirements.txt"
    generate_requirements(dags_dir, output_file)
    print(f"Requirements written to {output_file}")
