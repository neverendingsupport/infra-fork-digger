#!/usr/bin/env python3
"""Exercise the source-build shell embedded in action.yml."""

from pathlib import Path
from itertools import product
import os
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
ACTION = ROOT / "action.yml"


def source_build_script():
    """Extract the run block for the production source-build step."""
    lines = ACTION.read_text().splitlines()
    step = lines.index("    - name: build and run digger")
    run = lines.index("      run: |", step)
    body = []
    for line in lines[run + 1 :]:
        if line and not line.startswith("        "):
            break
        body.append(line[8:] if line.startswith("        ") else "")
    return "\n".join(body).replace(
        "${{ inputs.ee }}", '"${TEST_EE}"'
    ).replace("${{ inputs.fips }}", '"${TEST_FIPS}"')


class SourceBuildActionTest(unittest.TestCase):
    def test_compiled_binary_wins_over_older_path_binary(self):
        for ee, suffix in product(("false", "true"), ("-path", " path")):
            with self.subTest(ee=ee, suffix=suffix), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                action_path = root / f"action{suffix}"
                workspace = root / f"workspace{suffix}"
                earlier_path = root / "older cli"
                build_dir = action_path / ("ee/cli" if ee == "true" else "cli")
                for directory in (build_dir, workspace, earlier_path):
                    directory.mkdir(parents=True, exist_ok=True)

                old_cli = earlier_path / "digger"
                old_cli.write_text("#!/bin/bash\necho old > \"$MARKER\"\n")
                old_cli.chmod(0o755)

                go = earlier_path / "go"
                go.write_text(
                    "#!/bin/bash\n"
                    "[[ $1 == build && $2 == -o && $3 == digger ]] || exit 9\n"
                    "cat > digger <<'CLI'\n"
                    "#!/bin/bash\n"
                    "echo compiled > \"$MARKER\"\n"
                    "pwd > \"$WORKSPACE_MARKER\"\n"
                    "CLI\n"
                )
                go.chmod(0o755)

                env = os.environ.copy()
                env.update(
                    GITHUB_ACTION_PATH=str(action_path),
                    GITHUB_WORKSPACE=str(workspace),
                    PATH=f"{earlier_path}:{env['PATH']}",
                    MARKER=str(root / "selected"),
                    WORKSPACE_MARKER=str(root / "executed-in"),
                    TEST_EE=ee,
                    TEST_FIPS="false",
                )
                subprocess.run(
                    ["bash", "-e", "-u", "-o", "pipefail", "-c", source_build_script()],
                    cwd=ROOT,
                    env=env,
                    check=True,
                )
                self.assertEqual((root / "selected").read_text().strip(), "compiled")
                self.assertEqual(
                    Path((root / "executed-in").read_text().strip()), workspace
                )


if __name__ == "__main__":
    unittest.main()
