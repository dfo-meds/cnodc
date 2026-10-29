import pathlib


docs = pathlib.Path(__file__).absolute().resolve().parent
src_root = docs.parent / "src"


all_modules = {}

for file in src_root.glob("**/*.py"):
    if file.name in ("__init__.py", "__main__.py"):
        continue
    base_name = str(file)[len(str(src_root))+1:-3].replace("\\", "/")
    module_name = base_name.replace("/", ".")
    output_name = base_name.replace("/", "__") + ".rst"
    output_file = (docs / "modules" / base_name).with_suffix(".rst")
    if not output_file.exists():
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as h:
            h.write(f".. automodule:: {module_name}\n    :members:\n")
    module_pieces = module_name.split(".")
    next_dict = all_modules
    while module_pieces:
        next_dict_name = module_pieces.pop(0)
        if next_dict_name not in next_dict:
            next_dict[next_dict_name] = {}
        next_dict = next_dict[next_dict_name]
