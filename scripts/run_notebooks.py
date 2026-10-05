import argparse
import subprocess
import sys
import uuid
from pathlib import Path

import nbformat
from jupyter_client.kernelspec import KernelSpecManager
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parent.parent
KERNEL = "lakehouse"


# register the project venv as a jupyter kernel when missing
def ensure_kernel() -> None:
    if KERNEL in KernelSpecManager().find_kernel_specs():
        return
    subprocess.run(
        [sys.executable, "-m", "ipykernel", "install", "--user",
         "--name", KERNEL, "--display-name", "lakehouse"],
        check=True,
    )


# execute one notebook in place and keep its outputs
def run_notebook(path: Path, timeout: int) -> None:
    print(f"running {path.name} ...", flush=True)
    notebook = nbformat.read(path, as_version=4)
    for cell in notebook.cells:
        cell.setdefault("id", uuid.uuid4().hex[:8])
    client = NotebookClient(notebook, timeout=timeout, kernel_name=KERNEL,
                            resources={"metadata": {"path": str(path.parent)}})
    client.execute()
    nbformat.write(notebook, path)
    print(f"done {path.name}", flush=True)


# entry point: execute every notebook under notebooks/
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument("--only", default="")
    args = parser.parse_args()
    ensure_kernel()
    paths = sorted((ROOT / "notebooks").glob("*.ipynb"))
    if args.only:
        wanted = {name.strip() for name in args.only.split(",")}
        paths = [path for path in paths if path.stem in wanted]
    for path in paths:
        run_notebook(path, args.timeout)
    print(f"executed {len(paths)} notebook(s)")


if __name__ == "__main__":
    main()
