"""Guard coupled CodeQL actions and protected analysis coverage."""

import posixpath
import re
from fnmatch import fnmatchcase
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_codeql_init_and_analyze_use_one_pinned_release():
    pairs = 0
    for path in (ROOT / ".github/workflows").glob("*.yml"):
        workflow = yaml.safe_load(path.read_text())
        for name, job in workflow.get("jobs", {}).items():
            actions = [step.get("uses", "") for step in job.get("steps", [])]
            codeql = [action for action in actions if action.startswith("github/codeql-action/")]
            if not codeql:
                continue
            refs = {}
            for action in codeql:
                component, _, ref = action.partition("@")
                assert re.fullmatch(r"[a-f0-9]{40}", ref), (path.name, name, action)
                refs[component.rsplit("/", 1)[1]] = ref
            assert {"init", "analyze"} <= refs.keys(), (path.name, name, refs)
            assert len(set(refs.values())) == 1, (path.name, name, refs)
            pairs += 1
    assert pairs > 0, "No CodeQL analyses found"


def test_dependabot_keeps_codeql_components_in_one_group():
    config = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    updates = [item for item in config["updates"] if item["package-ecosystem"] == "github-actions"]
    assert len(updates) == 1
    groups = updates[0]["groups"]
    for component in ("init", "analyze", "autobuild", "upload-sarif"):
        dependency = f"github/codeql-action/{component}"
        matches = {
            name
            for name, group in groups.items()
            if any(fnmatchcase(dependency, pattern) for pattern in group.get("patterns", []))
            and not any(
                fnmatchcase(dependency, pattern) for pattern in group.get("exclude-patterns", [])
            )
        }
        assert matches == {"codeql"}, (dependency, matches)


def test_dependabot_preserves_supported_compiler_api_and_node_minimum():
    config = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    updates = [item for item in config["updates"] if item["package-ecosystem"] == "bun"]
    assert len(updates) == 1
    ignored = updates[0].get("ignore", [])
    assert {item["dependency-name"] for item in ignored} == {"typescript", "commander"}
    for item in ignored:
        assert item.get("update-types") == ["version-update:semver-major"]
        assert "versions" not in item, "Patch and minor security repairs must remain eligible"


def _python_update_directories(update):
    # Dependabot expands `directories` globs differently from fnmatch, so Python updates
    # must name literal directories for lockfile coverage to stay checkable.
    directories = update.get("directories") or [update["directory"]]
    for directory in directories:
        assert not set(directory) & set("*?[]{}"), f"Use literal Python directories: {directory!r}"
    return {posixpath.normpath("/" + directory.lstrip("/")) for directory in directories}


def test_dependabot_python_updates_keep_uv_lock_in_sync():
    config = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    python_updates = [
        item for item in config["updates"] if item["package-ecosystem"] in {"pip", "uv"}
    ]
    root = [
        item["package-ecosystem"]
        for item in python_updates
        if "/" in _python_update_directories(item)
    ]
    assert "pip" not in root, "pip updates leave uv.lock stale and fail locked installs"
    assert root.count("uv") == 1
    assert (ROOT / "uv.lock").is_file()


def test_required_codeql_context_requires_both_languages():
    workflow = yaml.safe_load((ROOT / ".github/workflows/codeql.yml").read_text())
    analysis = workflow["jobs"]["analyze"]
    assert analysis["name"] == "Analyze (${{ matrix.language }})"
    assert set(analysis["strategy"]["matrix"]["language"]) == {"javascript-typescript", "python"}
    assert analysis["strategy"]["fail-fast"] is False
    required = workflow["jobs"]["required"]
    assert required["name"] == "CodeQL"
    assert required["needs"] == "analyze"
    assert required["if"] == "${{ always() }}"
    step = required["steps"][0]
    assert step["env"]["ANALYZE_RESULT"] == "${{ needs.analyze.result }}"
    assert step["run"] == 'test "$ANALYZE_RESULT" = success'
