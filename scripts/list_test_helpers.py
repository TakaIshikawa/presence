#!/usr/bin/env python3
"""Generate comprehensive inventory of test helpers with usage examples.

This script analyzes the tests/helpers package to:
- List all available helper functions and classes
- Extract docstrings and function signatures
- Generate usage examples
- Detect orphaned helpers (defined but never used)
- Create helper documentation for README

Usage:
    python scripts/list_test_helpers.py                    # Print inventory
    python scripts/list_test_helpers.py --format json      # JSON output
    python scripts/list_test_helpers.py --detect-orphans   # Find unused helpers
    python scripts/list_test_helpers.py --generate-docs    # Generate markdown docs
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import Any

# Add src/ to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def extract_function_info(module_path: Path) -> list[dict[str, Any]]:
    """Extract function information from a Python module.

    Args:
        module_path: Path to the Python module file

    Returns:
        List of dicts containing function metadata
    """
    with open(module_path) as f:
        tree = ast.parse(f.read(), filename=str(module_path))

    functions = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Skip private functions
            if node.name.startswith("_"):
                continue

            # Extract docstring
            docstring = ast.get_docstring(node)

            # Extract parameters
            params = []
            for arg in node.args.args:
                param_name = arg.arg
                # Extract type annotation if present
                annotation = ""
                if arg.annotation:
                    annotation = ast.unparse(arg.annotation)
                params.append({"name": param_name, "type": annotation})

            # Extract return type annotation
            return_type = ""
            if node.returns:
                return_type = ast.unparse(node.returns)

            functions.append(
                {
                    "name": node.name,
                    "docstring": docstring or "",
                    "params": params,
                    "return_type": return_type,
                    "lineno": node.lineno,
                }
            )

    return functions


def find_helper_usages(tests_dir: Path, helper_name: str) -> list[str]:
    """Find all test files that use a specific helper.

    Args:
        tests_dir: Path to tests directory
        helper_name: Name of the helper function to search for

    Returns:
        List of test file paths that import or use the helper
    """
    usages = []

    for test_file in tests_dir.glob("test_*.py"):
        try:
            with open(test_file) as f:
                content = f.read()
                if helper_name in content:
                    usages.append(str(test_file.name))
        except OSError:
            continue

    return usages


def generate_inventory(
    helpers_dir: Path, tests_dir: Path, include_usage: bool = True
) -> dict[str, Any]:
    """Generate comprehensive inventory of all test helpers.

    Args:
        helpers_dir: Path to tests/helpers directory
        tests_dir: Path to tests directory
        include_usage: Whether to search for usage examples

    Returns:
        Dictionary containing helper inventory
    """
    inventory = {
        "version": "1.0.0",
        "modules": {},
        "total_helpers": 0,
    }

    # Process each Python file in helpers directory
    for module_path in helpers_dir.glob("*.py"):
        if module_path.name.startswith("_") and module_path.name != "__init__.py":
            continue

        if module_path.name == "__init__.py":
            continue

        module_name = module_path.stem
        functions = extract_function_info(module_path)

        # Add usage information if requested
        if include_usage:
            for func in functions:
                func["used_in"] = find_helper_usages(tests_dir, func["name"])
                func["is_orphaned"] = len(func["used_in"]) == 0

        inventory["modules"][module_name] = {
            "path": str(module_path.relative_to(tests_dir.parent)),
            "functions": functions,
            "count": len(functions),
        }
        inventory["total_helpers"] += len(functions)

    return inventory


def format_inventory_text(inventory: dict[str, Any]) -> str:
    """Format inventory as human-readable text.

    Args:
        inventory: Helper inventory dictionary

    Returns:
        Formatted text output
    """
    lines = [
        "Test Helpers Inventory",
        "=" * 80,
        f"Total Helpers: {inventory['total_helpers']}",
        f"Version: {inventory['version']}",
        "",
    ]

    for module_name, module_info in inventory["modules"].items():
        lines.append(f"\nModule: {module_name}")
        lines.append("-" * 80)
        lines.append(f"Path: {module_info['path']}")
        lines.append(f"Functions: {module_info['count']}")
        lines.append("")

        for func in module_info["functions"]:
            lines.append(f"  {func['name']}()")
            if func["docstring"]:
                first_line = func["docstring"].split("\n")[0]
                lines.append(f"    {first_line}")

            # Show parameters
            if func["params"]:
                param_str = ", ".join(
                    f"{p['name']}: {p['type']}" if p["type"] else p["name"]
                    for p in func["params"]
                )
                lines.append(f"    Parameters: {param_str}")

            # Show usage
            if "used_in" in func:
                if func["is_orphaned"]:
                    lines.append("    ⚠️  ORPHANED (not used in any tests)")
                else:
                    lines.append(f"    Used in: {', '.join(func['used_in'][:3])}")
                    if len(func["used_in"]) > 3:
                        lines.append(
                            f"             ... and {len(func['used_in']) - 3} more"
                        )

            lines.append("")

    return "\n".join(lines)


def format_inventory_json(inventory: dict[str, Any]) -> str:
    """Format inventory as JSON.

    Args:
        inventory: Helper inventory dictionary

    Returns:
        JSON string
    """
    return json.dumps(inventory, indent=2)


def generate_markdown_docs(inventory: dict[str, Any]) -> str:
    """Generate markdown documentation from inventory.

    Args:
        inventory: Helper inventory dictionary

    Returns:
        Markdown documentation string
    """
    lines = [
        "# Test Helpers Reference",
        "",
        f"**Version:** {inventory['version']}  ",
        f"**Total Helpers:** {inventory['total_helpers']}",
        "",
        "## Available Helpers",
        "",
    ]

    for module_name, module_info in inventory["modules"].items():
        lines.append(f"### Module: `{module_name}`")
        lines.append("")
        lines.append(f"**Path:** `{module_info['path']}`  ")
        lines.append(f"**Functions:** {module_info['count']}")
        lines.append("")

        for func in module_info["functions"]:
            # Function signature
            param_str = ", ".join(
                f"{p['name']}: {p['type']}" if p["type"] else p["name"]
                for p in func["params"]
            )
            return_annotation = f" -> {func['return_type']}" if func["return_type"] else ""
            lines.append(f"#### `{func['name']}({param_str}){return_annotation}`")
            lines.append("")

            # Docstring
            if func["docstring"]:
                lines.append(func["docstring"])
                lines.append("")

            # Usage statistics
            if "used_in" in func and not func["is_orphaned"]:
                lines.append(f"**Used in:** {len(func['used_in'])} test file(s)")
                lines.append("")

        lines.append("")

    return "\n".join(lines)


def detect_orphaned_helpers(inventory: dict[str, Any]) -> list[dict[str, str]]:
    """Detect helpers that are defined but never used.

    Args:
        inventory: Helper inventory dictionary

    Returns:
        List of orphaned helper information
    """
    orphaned = []

    for module_name, module_info in inventory["modules"].items():
        for func in module_info["functions"]:
            if func.get("is_orphaned", False):
                orphaned.append(
                    {
                        "module": module_name,
                        "function": func["name"],
                        "path": module_info["path"],
                        "lineno": func["lineno"],
                    }
                )

    return orphaned


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--format",
        choices=("text", "json", "markdown"),
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--detect-orphans",
        action="store_true",
        help="Only show orphaned helpers (defined but never used)",
    )
    parser.add_argument(
        "--generate-docs",
        action="store_true",
        help="Generate markdown documentation for README",
    )
    parser.add_argument(
        "--no-usage",
        action="store_true",
        help="Skip usage detection (faster but less informative)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    # Determine paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    tests_dir = project_root / "tests"
    helpers_dir = tests_dir / "helpers"

    if not helpers_dir.exists():
        print(f"error: helpers directory not found: {helpers_dir}", file=sys.stderr)
        return 1

    # Generate inventory
    try:
        inventory = generate_inventory(
            helpers_dir, tests_dir, include_usage=not args.no_usage
        )
    except (OSError, SyntaxError) as exc:
        print(f"error: failed to generate inventory: {exc}", file=sys.stderr)
        return 1

    # Handle different output modes
    if args.detect_orphans:
        orphaned = detect_orphaned_helpers(inventory)
        if not orphaned:
            print("✓ No orphaned helpers found. All helpers are in use!")
            return 0

        print(f"⚠️  Found {len(orphaned)} orphaned helper(s):")
        print()
        for helper in orphaned:
            print(f"  {helper['module']}.{helper['function']}")
            print(f"    Location: {helper['path']}:{helper['lineno']}")
        return 0

    if args.generate_docs:
        print(generate_markdown_docs(inventory))
        return 0

    # Normal output
    if args.format == "json":
        print(format_inventory_json(inventory))
    elif args.format == "markdown":
        print(generate_markdown_docs(inventory))
    else:
        print(format_inventory_text(inventory))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
