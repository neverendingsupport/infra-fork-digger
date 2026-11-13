"""Run the shell embedded in action.yml's steps, for the test_action_* tests.

Needs PyYAML (python3-yaml on Ubuntu runners).
"""

from pathlib import Path
import os
import subprocess
import tempfile

import yaml


ROOT = Path(__file__).resolve().parents[1]
STEPS = yaml.safe_load((ROOT / "action.yml").read_text())["runs"]["steps"]


def step(step_id):
    for s in STEPS:
        if s.get("id") == step_id:
            return s
    raise AssertionError(f"action.yml has no step with id {step_id!r}")


def step_named(name):
    for s in STEPS:
        if s.get("name") == name:
            return s
    raise AssertionError(f"action.yml has no step named {name!r}")


def write_executable(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(0o755)
    return path


def _read_pairs(path):
    return dict(line.split("=", 1) for line in path.read_text().splitlines())


def run_script(script, env, cwd=ROOT):
    """Run a step script; return the result and its GITHUB_OUTPUT and GITHUB_ENV."""
    with tempfile.TemporaryDirectory() as scratch:
        output = Path(scratch) / "output"
        github_env = Path(scratch) / "env"
        output.touch()
        github_env.touch()
        full_env = os.environ.copy()
        full_env.update(env, GITHUB_OUTPUT=str(output), GITHUB_ENV=str(github_env))
        result = subprocess.run(["bash", "-c", script], cwd=cwd, env=full_env, capture_output=True, text=True)
        return result, _read_pairs(output), _read_pairs(github_env)


def run_step(step_id, env, cwd=ROOT):
    return run_script(step(step_id)["run"], env, cwd)
