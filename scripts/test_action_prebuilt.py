#!/usr/bin/env python3
"""Exercise the prebuilt-CLI download shell embedded in action.yml."""

from pathlib import Path
import hashlib
import os
import shutil
import sys
import tempfile
import unittest

from action_harness import run_step, step, write_executable


FAKE_CLI = '#!/bin/bash\necho prebuilt > "$MARKER"\npwd > "$WORKSPACE_MARKER"\n'
FAKE_SHA = hashlib.sha256(FAKE_CLI.encode()).hexdigest()

# Records the URL and writes FAKE_CLI to -o. CURL_FAIL acts like an HTTP error.
FAKE_CURL = (
    "#!/bin/bash\n"
    'while [[ $# -gt 0 ]]; do case "$1" in -o) out="$2"; shift 2 ;; --fail) fail=1; shift ;; https://*) echo "$1" > "$URL_MARKER"; shift ;; *) shift ;; esac; done\n'
    'if [[ -n "${CURL_FAIL:-}" ]]; then [[ -n "${fail:-}" ]] && exit 22; echo "Not Found" > "$out"; exit 0; fi\n'
    'if [[ -n "${CURL_EMPTY:-}" ]]; then : > "$out"; else cat "$FAKE_CLI_FILE" > "$out"; fi\n'
)


class BinaryModeTest(unittest.TestCase):
    def mode(self, version, sha="", action_ref="0123abc"):
        result, output, _ = run_step(
            "determine-binary-mode",
            {"LOCAL_DEV_MODE": "false", "INPUT_DIGGER_VERSION": version, "INPUT_DIGGER_CLI_SHA256": sha, "ACTION_REF": action_ref},
        )
        return result.returncode, output.get("binary-mode")

    def test_accepts_release_tags(self):
        for version in ("v0.6.152", "nes/cli-ee/v0.6.152-nes.1", "v1.2.3-rc.1"):
            with self.subTest(version=version):
                self.assertEqual(self.mode(version), (0, "prebuilt"))
        self.assertEqual(self.mode(""), (0, "build"))
        self.assertEqual(self.mode("", action_ref="vLatest"), (0, "prebuilt"))

    def test_rejects_anything_else(self):
        for version in ("v1.2", "../v1.2.3", "nes/../v1.2.3", "nes//v1.2.3", "v1.2.3-", "v1.2.3-a/b", "a b/v1.2.3", "v1.2.3/x", "v1.2.3?x"):
            with self.subTest(version=version):
                self.assertEqual(self.mode(version), (1, None))

    def test_refuses_a_checksum_it_would_ignore(self):
        self.assertEqual(self.mode("", sha=FAKE_SHA), (1, None))
        self.assertEqual(self.mode("v0.6.152", sha=FAKE_SHA), (0, "prebuilt"))


class InstallTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        (self.root / "runner-temp").mkdir()
        (self.root / "cli").write_text(FAKE_CLI)
        # A checked-out file named digger must survive the install untouched.
        write_executable(self.workspace / "digger", "planted\n")
        self.bin_dir = self.root / "bin"
        write_executable(self.bin_dir / "curl", FAKE_CURL)

    def install(self, path=None, **inputs):
        env = {
            "PATH": path or f"{self.bin_dir}:{os.environ['PATH']}",
            "GITHUB_WORKSPACE": str(self.workspace),
            "RUNNER_TEMP": str(self.root / "runner-temp"),
            "RUNNER_OS": "Linux",
            "RUNNER_ARCH": "ARM64",
            "URL_MARKER": str(self.root / "url"),
            "FAKE_CLI_FILE": str(self.root / "cli"),
            "DIGGER_VERSION": "nes/cli-ee/v0.6.152-nes.1",
            "DIGGER_OS": "linux",
            "DIGGER_ARCH": "arm64",
            "DIGGER_RELEASE_REPOSITORY": "neverendingsupport/infra-fork-digger",
            "DIGGER_CLI_SHA256": FAKE_SHA,
            "DIGGER_EE": "true",
            "DIGGER_FIPS": "false",
        }
        env.update(inputs)
        result, output, _ = run_step("install-prebuilt", env, cwd=self.root)
        url_file = self.root / "url"
        url = url_file.read_text().strip() if url_file.exists() else None
        url_file.unlink(missing_ok=True)
        self.assertEqual([p.name for p in self.workspace.iterdir()], ["digger"])
        self.assertEqual((self.workspace / "digger").read_text(), "planted\n")
        return result, output.get("bin"), url

    def assert_installed(self, result, binary):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(Path(binary).read_text(), FAKE_CLI)
        self.assertTrue(os.access(binary, os.X_OK))
        self.assertTrue(Path(binary).is_relative_to(self.root / "runner-temp"), binary)

    def test_installs_a_binary_that_matches_the_checksum(self):
        for sha in (FAKE_SHA, FAKE_SHA.upper()):
            with self.subTest(sha=sha[:8]):
                result, binary, url = self.install(DIGGER_CLI_SHA256=sha)
                self.assert_installed(result, binary)
                self.assertEqual(
                    url,
                    "https://github.com/neverendingsupport/infra-fork-digger/releases/download/"
                    "nes/cli-ee/v0.6.152-nes.1/digger-ee-cli-linux-arm64",
                )
                self.assertIn("checksum verified", result.stdout)

    def test_checks_with_shasum_when_sha256sum_is_missing(self):
        tools = self.root / "tools"
        for tool in ("bash", "tr", "mktemp", "cut", "chmod", "cat"):
            write_executable(tools / tool, f'#!/bin/sh\nexec {shutil.which(tool)} "$@"\n')
        write_executable(tools / "curl", FAKE_CURL)
        python = sys.executable
        write_executable(
            tools / "shasum",
            f"#!{python}\nimport hashlib, sys\nassert sys.argv[1:] == ['-a', '256']\n"
            "print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest() + '  -')\n",
        )
        result, binary, _ = self.install(path=str(tools))
        self.assert_installed(result, binary)
        result, binary, _ = self.install(path=str(tools), DIGGER_CLI_SHA256="0" * 64)
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(binary)

    def test_refuses_a_binary_that_does_not_match(self):
        result, binary, _ = self.install(DIGGER_CLI_SHA256="0" * 64)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("checksum mismatch", result.stdout)
        self.assertIsNone(binary)

    def test_checksum_is_optional_only_for_upstream_releases(self):
        result, binary, _ = self.install(DIGGER_CLI_SHA256="", DIGGER_RELEASE_REPOSITORY="diggerhq/digger")
        self.assert_installed(result, binary)
        result, binary, url = self.install(DIGGER_CLI_SHA256="")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("digger-cli-sha256 is required", result.stdout)
        self.assertIsNone(url)

    def test_fails_when_the_download_fails_or_is_empty(self):
        for inputs in ({"CURL_FAIL": "1"}, {"CURL_EMPTY": "1"}):
            with self.subTest(**inputs):
                # Without a checksum, so only these checks can catch it.
                result, binary, _ = self.install(DIGGER_CLI_SHA256="", DIGGER_RELEASE_REPOSITORY="diggerhq/digger", **inputs)
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(binary)

    def test_names_the_asset_for_each_variant(self):
        cases = [
            ({"DIGGER_FIPS": "true"}, "digger-ee-cli-linux-arm64-fips"),
            ({"DIGGER_EE": "false"}, "digger-cli-linux-arm64"),
            ({"DIGGER_EE": "false", "DIGGER_FIPS": "true"}, "digger-cli-linux-arm64"),
            ({"DIGGER_OS": "windows", "DIGGER_ARCH": "amd64.exe"}, "digger-ee-cli-windows-amd64.exe"),
            ({"DIGGER_VERSION": "vLatest", "DIGGER_OS": "Linux", "DIGGER_ARCH": "X64"},
             "https://github.com/diggerhq/digger/releases/download/vLatest/digger-ee-cli-Linux-X64"),
        ]
        for inputs, want in cases:
            with self.subTest(**inputs):
                result, _, url = self.install(**{"DIGGER_CLI_SHA256": "", "DIGGER_RELEASE_REPOSITORY": "diggerhq/digger", **inputs})
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertTrue(url.endswith(want), url)

    def test_rejects_bad_inputs_before_downloading(self):
        cases = [
            {"DIGGER_CLI_SHA256": "abc"},
            {"DIGGER_CLI_SHA256": "g" * 64},
            {"DIGGER_RELEASE_REPOSITORY": "evil.example/x/y"},
            {"DIGGER_RELEASE_REPOSITORY": "noslash"},
            {"DIGGER_RELEASE_REPOSITORY": "../x"},
            {"DIGGER_RELEASE_REPOSITORY": "x/.."},
            {"DIGGER_OS": "linux/x"},
            {"DIGGER_ARCH": "arm64/../x"},
            {"DIGGER_VERSION": "../v1.2.3"},
            {"DIGGER_VERSION": "a/./v1.2.3"},
            {"DIGGER_VERSION": "v1.2.3?x"},
            {"DIGGER_VERSION": "/v1.2.3"},
        ]
        for inputs in cases:
            with self.subTest(**inputs):
                result, binary, url = self.install(**inputs)
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(url)
                self.assertIsNone(binary)

    def test_settings_reach_only_the_install_step(self):
        settings = {
            "DIGGER_VERSION": "${{ inputs.digger-version || github.action_ref }}",
            "DIGGER_OS": "${{ inputs.digger-os }}",
            "DIGGER_ARCH": "${{ inputs.digger-arch }}",
            "DIGGER_RELEASE_REPOSITORY": "${{ inputs.digger-release-repository }}",
            "DIGGER_CLI_SHA256": "${{ inputs.digger-cli-sha256 }}",
            "DIGGER_EE": "${{ inputs.ee }}",
            "DIGGER_FIPS": "${{ inputs.fips }}",
        }
        self.assertEqual(step("install-prebuilt")["env"], settings)
        self.assertEqual(set(settings) & set(step("digger")["env"]), set())


class RunTest(unittest.TestCase):
    def test_runs_the_installed_binary_from_the_workspace(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            workspace = root / "workspace"
            workspace.mkdir()
            cli = write_executable(root / "runner-temp" / "digger", FAKE_CLI)
            # An older digger on PATH must not run instead.
            write_executable(root / "bin" / "digger", '#!/bin/bash\necho path > "$MARKER"\n')
            result, _, _ = run_step(
                "digger",
                {
                    "PATH": f"{root / 'bin'}:{os.environ['PATH']}",
                    "PREBUILT_CLI": str(cli),
                    "GITHUB_WORKSPACE": str(workspace),
                    "MARKER": str(root / "ran"),
                    "WORKSPACE_MARKER": str(root / "ran-in"),
                },
                cwd=root,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((root / "ran").read_text().strip(), "prebuilt")
            self.assertEqual(Path((root / "ran-in").read_text().strip()), workspace)

    def test_runs_what_the_install_step_installed(self):
        run = step("digger")
        self.assertEqual(run["env"]["PREBUILT_CLI"], "${{ steps.install-prebuilt.outputs.bin }}")
        self.assertEqual(run["if"], step("install-prebuilt")["if"])


if __name__ == "__main__":
    unittest.main()
