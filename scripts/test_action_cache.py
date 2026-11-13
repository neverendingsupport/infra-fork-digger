#!/usr/bin/env python3
"""Exercise the provider-cache shell embedded in action.yml."""

from pathlib import Path
import os
import tempfile
import unittest

from action_harness import STEPS, run_step, step, step_named


class CacheSettingsTest(unittest.TestCase):
    def settings(self, provider_cache, cache_dependencies="false", cache_dependencies_s3="false", cache_dir="/cache dir"):
        return run_step(
            "cache-settings",
            {
                "CACHE_DIR": cache_dir,
                "TG_PROVIDER_CACHE_INPUT": provider_cache,
                "CACHE_DEPENDENCIES": cache_dependencies,
                "CACHE_DEPENDENCIES_S3": cache_dependencies_s3,
                "RUNNER_OS": "Linux",
                "RUNNER_ARCH": "ARM64",
            },
        )

    def test_provider_cache_follows_input_then_either_cache_option(self):
        cases = [
            ("", "false", "false", "0"),
            ("", "true", "false", "1"),
            ("", "false", "true", "1"),
            ("", "TRUE", "false", "1"),
            ("true", "false", "false", "1"),
            ("True", "false", "false", "1"),
            ("false", "true", "true", "0"),
        ]
        for provider_cache, actions_cache, s3_cache, want in cases:
            with self.subTest(input=provider_cache, actions=actions_cache, s3=s3_cache):
                result, output, github_env = self.settings(provider_cache, actions_cache, s3_cache)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(output["tg-provider-cache"], want)
                self.assertEqual(output["cache-key-prefix"], "digger-cache-Linux-ARM64-")
                self.assertEqual(output["cache-dir"], "/cache dir")
                for name in ("TF_PLUGIN_CACHE_DIR", "TG_PROVIDER_CACHE_DIR", "TERRAGRUNT_PROVIDER_CACHE_DIR"):
                    self.assertEqual(github_env[name], "/cache dir")

    def test_rejects_other_values_before_writing_anything(self):
        result, output, github_env = self.settings("yes", "true")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid terragrunt-provider-cache", result.stdout)
        self.assertEqual(output, {})
        self.assertEqual(github_env, {})

    def test_rejects_a_cache_dir_with_a_line_break(self):
        for cache_dir in ("/x\nBASH_ENV=/tmp/y", "/x\rY=1"):
            with self.subTest(cache_dir=cache_dir):
                result, output, github_env = self.settings("", "true", cache_dir=cache_dir)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(output, {})
                self.assertEqual(github_env, {})

    def test_expands_a_leading_tilde_like_the_shell_did(self):
        home = os.environ["HOME"]
        cases = [("~", home), ("~/providers", f"{home}/providers"), ("/a/~/b", "/a/~/b"), ("~user/x", "~user/x")]
        for cache_dir, want in cases:
            with self.subTest(cache_dir=cache_dir):
                result, output, github_env = self.settings("", cache_dir=cache_dir)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(output["cache-dir"], want)
                self.assertEqual(github_env["TF_PLUGIN_CACHE_DIR"], want)

    def test_inputs_reach_the_script_unchanged(self):
        self.assertEqual(
            step("cache-settings")["env"],
            {
                "CACHE_DIR": "${{ inputs.terraform-cache-dir || format('{0}/cache', github.workspace) }}",
                "TG_PROVIDER_CACHE_INPUT": "${{ inputs.terragrunt-provider-cache }}",
                "CACHE_DEPENDENCIES": "${{ inputs.cache-dependencies }}",
                "CACHE_DEPENDENCIES_S3": "${{ inputs.cache-dependencies-s3 }}",
            },
        )

    def test_every_run_step_uses_the_decision(self):
        want = "${{ steps.cache-settings.outputs.tg-provider-cache }}"
        users = [s for s in STEPS if "TG_PROVIDER_CACHE" in s.get("env", {})]
        self.assertEqual(len(users), 3)
        for s in users:
            with self.subTest(step=s["name"]):
                self.assertEqual(s["env"]["TG_PROVIDER_CACHE"], want)
                self.assertEqual(s["env"]["TERRAGRUNT_PROVIDER_CACHE"], want)


PREFIX = "digger-cache-Linux-ARM64-"


def add_provider(cache, version):
    path = cache / f"registry.terraform.io/hashicorp/aws/{version}/linux_arm64/terraform-provider-aws"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(version)
    return path


class CacheKeyTest(unittest.TestCase):
    def key(self, cache_dir, restored="", prefix=PREFIX, event="pull_request"):
        result, output, _ = run_step(
            "cache-key",
            {"KEY_PREFIX": prefix, "RESTORED_KEY": restored, "CACHE_DIR": str(cache_dir), "GITHUB_EVENT_NAME": event},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return output.get("key", "")

    def test_key_changes_only_with_the_provider_set(self):
        with tempfile.TemporaryDirectory() as temp:
            cache = Path(temp) / "cache"
            provider = add_provider(cache, "6.0.0")
            first = self.key(cache)
            self.assertRegex(first, rf"^{PREFIX}[0-9a-f]{{16}}$")

            provider.write_text("same provider, rewritten")
            self.assertEqual(self.key(cache), first)

            add_provider(cache, "6.1.0")
            self.assertNotEqual(self.key(cache), first)

    def test_key_ignores_where_the_cache_dir_is(self):
        with tempfile.TemporaryDirectory() as one, tempfile.TemporaryDirectory() as two:
            add_provider(Path(one) / "cache", "6.0.0")
            add_provider(Path(two) / "elsewhere" / "cache", "6.0.0")
            self.assertEqual(self.key(Path(one) / "cache"), self.key(Path(two) / "elsewhere" / "cache"))

    def test_writes_no_key_when_there_is_nothing_new_to_save(self):
        with tempfile.TemporaryDirectory() as temp:
            cache = Path(temp) / "cache"
            self.assertEqual(self.key(cache), "", "missing dir")
            cache.mkdir()
            self.assertEqual(self.key(cache), "", "empty dir")
            add_provider(cache, "6.0.0")
            restored = self.key(cache)
            self.assertEqual(self.key(cache, restored=restored), "", "unchanged since restore")
            self.assertEqual(self.key(cache, prefix=""), "", "settings step failed")
            self.assertEqual(self.key(cache, event="issue_comment"), "", "comment run")
            for event in ("push", "pull_request", "workflow_dispatch", "schedule"):
                self.assertNotEqual(self.key(cache, event=event), "", event)

    def test_save_runs_only_with_a_key(self):
        save = step_named("cache-save")
        cache_dir = "${{ steps.cache-settings.outputs.cache-dir }}"
        self.assertEqual(save["if"], "${{ success() && steps.cache-key.outputs.key != '' }}")
        self.assertEqual(save["with"], {"path": cache_dir, "key": "${{ steps.cache-key.outputs.key }}"})
        self.assertEqual(step("cache-key")["env"]["CACHE_DIR"], cache_dir)
        self.assertEqual(step("restore_cache")["with"]["path"], cache_dir)
        self.assertEqual(
            step("cache-key")["env"]["RESTORED_KEY"],
            "${{ steps.restore_cache.outputs.cache-matched-key }}",
        )
        enabled = "${{ inputs.cache-dependencies == 'true' }}"
        self.assertEqual(step("cache-key")["if"], "${{ success() && inputs.cache-dependencies == 'true' }}")
        restore = step("restore_cache")
        self.assertEqual(restore["if"], enabled)
        prefix = "${{ steps.cache-settings.outputs.cache-key-prefix }}"
        self.assertEqual(restore["with"]["key"], prefix)
        self.assertEqual(restore["with"]["restore-keys"].strip(), prefix)


if __name__ == "__main__":
    unittest.main()
