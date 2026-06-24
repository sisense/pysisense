#!/usr/bin/env python3
"""Docstring convention checker for the pysisense SDK.

Validates that every public method and facade class follows the docstring
conventions defined in ``CLAUDE.md``. The checker is generic: it discovers
methods and classes by parsing the source with :mod:`ast`, so it applies to
any method added in the future without per-method configuration.

Checks performed
----------------
- ``missing-docstring``     : public method / facade class has no docstring.
- ``params-section``        : a method taking arguments must have a NumPy
                              ``Parameters`` section.
- ``param-coverage``        : every signature parameter is documented and no
                              documented parameter is absent from the signature.
- ``returns-section``       : a method that does not return ``None`` must have
                              a NumPy ``Returns`` section.
- ``missing-type-hints``    : every parameter and the return value must be
                              annotated.
- ``bad-format-tag``        : any ``(format: X)`` marker must use the allowed
                              vocabulary (email, uuid, date, ipv4, ipv6).
- ``forbidden-term``        : docstrings must not mention MCP, LLMs, agents, or
                              include ``Example``/``Examples`` blocks.
- ``facade-modules``        : a class defined in ``__init__.py`` must document a
                              ``Modules`` section for navigation.

Exit code is non-zero when any violation is found.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "pysisense"

ALLOWED_FORMATS = {"email", "uuid", "date", "ipv4", "ipv6"}

# Parameters that never need documentation or type hints.
IGNORED_PARAMS = {"self", "cls"}

FORMAT_TAG_RE = re.compile(r"\(format:\s*([^)]*?)\s*\)")

# Word-boundary patterns for terms banned from SDK docstrings.
FORBIDDEN_TERM_RES = {
    "MCP": re.compile(r"\bmcp\b", re.IGNORECASE),
    "LLM": re.compile(r"\bllms?\b", re.IGNORECASE),
    "language model": re.compile(r"\blanguage model", re.IGNORECASE),
    "agent": re.compile(r"\bagents?\b", re.IGNORECASE),
    "Example block": re.compile(r"^\s*examples?\s*$", re.IGNORECASE | re.MULTILINE),
}

NUMPY_SECTIONS = {
    "Parameters",
    "Returns",
    "Yields",
    "Raises",
    "Notes",
    "Other Parameters",
    "Attributes",
    "Methods",
    "See Also",
    "References",
    "Warns",
    "Modules",
}


class Violation:
    """A single docstring-convention failure."""

    def __init__(self, path: Path, lineno: int, name: str, code: str, message: str) -> None:
        self.path = path
        self.lineno = lineno
        self.name = name
        self.code = code
        self.message = message

    def __str__(self) -> str:
        try:
            rel = self.path.resolve().relative_to(PACKAGE_ROOT.parent)
        except ValueError:
            rel = self.path
        return f"{rel}:{self.lineno}: [{self.code}] {self.name}: {self.message}"


def _section_body(doc: str, section: str) -> list[str] | None:
    """Return the lines of a NumPy section, or ``None`` if it is absent.

    Parameters
    ----------
    doc : str
        The full docstring text.
    section : str
        The NumPy section header to extract (for example ``"Parameters"``).

    Returns
    -------
    list[str] | None
        The lines that make up the section body, or ``None`` when the section
        header followed by an underline of dashes is not present.
    """
    lines = doc.splitlines()
    for i in range(len(lines) - 1):
        if lines[i].strip() == section and set(lines[i + 1].strip()) == {"-"} and lines[i + 1].strip():
            body: list[str] = []
            for line in lines[i + 2 :]:
                stripped = line.strip()
                # Stop at the next section header (header line + dashed underline).
                if stripped in NUMPY_SECTIONS:
                    break
                if stripped and set(stripped) == {"-"}:
                    body.pop() if body else None
                    break
                body.append(line)
            return body
    return None


def _documented_params(doc: str) -> set[str]:
    """Extract documented parameter names from a NumPy ``Parameters`` section.

    Parameters
    ----------
    doc : str
        The full docstring text.

    Returns
    -------
    set[str]
        The set of parameter names declared in the ``Parameters`` section. Empty
        when no such section exists.
    """
    body = _section_body(doc, "Parameters")
    if body is None:
        return set()
    names: set[str] = set()
    for line in body:
        # NumPy param declarations are not indented: ``name : type``.
        if line[:1] in (" ", "\t"):
            continue
        match = re.match(r"^(\*{0,2}\w+)\s*:", line)
        if match:
            names.add(match.group(1).lstrip("*"))
    return names


def _signature_params(func: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.arg]:
    """Return the documentable argument nodes of a function definition.

    Parameters
    ----------
    func : ast.FunctionDef | ast.AsyncFunctionDef
        The parsed function node.

    Returns
    -------
    list[ast.arg]
        Positional, positional-only, and keyword-only arguments, excluding
        ``self``/``cls`` and ``*args``/``**kwargs``.
    """
    args = func.args
    collected = [*args.posonlyargs, *args.args, *args.kwonlyargs]
    return [a for a in collected if a.arg not in IGNORED_PARAMS]


def _returns_none(func: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Return True when the function is annotated to return ``None``.

    Parameters
    ----------
    func : ast.FunctionDef | ast.AsyncFunctionDef
        The parsed function node.

    Returns
    -------
    bool
        True if the return annotation is the literal ``None``.
    """
    ret = func.returns
    if ret is None:
        return False
    if isinstance(ret, ast.Constant) and ret.value is None:
        return True
    return bool(isinstance(ret, ast.Name) and ret.id == "None")


def _check_method(path: Path, cls: ast.ClassDef, func: ast.FunctionDef | ast.AsyncFunctionDef) -> list[Violation]:
    """Validate a single public method against the docstring conventions.

    Parameters
    ----------
    path : Path
        The source file containing the method.
    cls : ast.ClassDef
        The class node that owns the method.
    func : ast.FunctionDef | ast.AsyncFunctionDef
        The method node to validate.

    Returns
    -------
    list[Violation]
        All violations found for this method.
    """
    name = f"{cls.name}.{func.name}"
    violations: list[Violation] = []
    params = _signature_params(func)
    doc = ast.get_docstring(func)

    if not doc:
        violations.append(Violation(path, func.lineno, name, "missing-docstring", "no docstring"))
        # Type hints can still be checked without a docstring.
        violations.extend(_check_type_hints(path, name, func, params))
        return violations

    if params and _section_body(doc, "Parameters") is None:
        violations.append(Violation(path, func.lineno, name, "params-section", "method takes arguments but has no NumPy 'Parameters' section"))
    else:
        documented = _documented_params(doc)
        signature_names = {a.arg for a in params}
        undocumented = signature_names - documented
        phantom = documented - signature_names
        if undocumented:
            violations.append(Violation(path, func.lineno, name, "param-coverage", f"undocumented parameter(s): {', '.join(sorted(undocumented))}"))
        if phantom:
            violations.append(Violation(path, func.lineno, name, "param-coverage", f"documented parameter(s) not in signature: {', '.join(sorted(phantom))}"))

    if not _returns_none(func) and _section_body(doc, "Returns") is None:
        violations.append(Violation(path, func.lineno, name, "returns-section", "method returns a value but has no NumPy 'Returns' section"))

    for raw in FORMAT_TAG_RE.findall(doc):
        if raw not in ALLOWED_FORMATS:
            violations.append(Violation(path, func.lineno, name, "bad-format-tag", f"unknown format tag '(format: {raw})' — allowed: {', '.join(sorted(ALLOWED_FORMATS))}"))

    for label, pattern in FORBIDDEN_TERM_RES.items():
        if pattern.search(doc):
            violations.append(Violation(path, func.lineno, name, "forbidden-term", f"docstring mentions banned term: {label}"))

    violations.extend(_check_type_hints(path, name, func, params))
    return violations


def _check_type_hints(path: Path, name: str, func: ast.FunctionDef | ast.AsyncFunctionDef, params: list[ast.arg]) -> list[Violation]:
    """Validate that every parameter and the return value is annotated.

    Parameters
    ----------
    path : Path
        The source file containing the method.
    name : str
        The qualified ``Class.method`` name used in messages.
    func : ast.FunctionDef | ast.AsyncFunctionDef
        The method node to validate.
    params : list[ast.arg]
        The documentable argument nodes for the method.

    Returns
    -------
    list[Violation]
        Any missing type-hint violations.
    """
    violations: list[Violation] = []
    unannotated = [a.arg for a in params if a.annotation is None]
    if unannotated:
        violations.append(Violation(path, func.lineno, name, "missing-type-hints", f"parameter(s) missing type hints: {', '.join(unannotated)}"))
    if func.returns is None:
        violations.append(Violation(path, func.lineno, name, "missing-type-hints", "missing return type hint"))
    return violations


def _check_facade(path: Path, cls: ast.ClassDef) -> list[Violation]:
    """Validate the class-level docstring of a facade class in ``__init__.py``.

    Parameters
    ----------
    path : Path
        The source file containing the class.
    cls : ast.ClassDef
        The facade class node to validate.

    Returns
    -------
    list[Violation]
        Violations for a missing docstring or a missing ``Modules`` section.
    """
    doc = ast.get_docstring(cls)
    if not doc:
        return [Violation(path, cls.lineno, cls.name, "missing-docstring", "facade class has no docstring")]
    if _section_body(doc, "Modules") is None:
        return [Violation(path, cls.lineno, cls.name, "facade-modules", "facade class docstring has no 'Modules' section")]
    return []


def check_file(path: Path) -> list[Violation]:
    """Run all docstring checks over a single source file.

    Parameters
    ----------
    path : Path
        The Python source file to validate.

    Returns
    -------
    list[Violation]
        Every violation discovered in the file.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    is_facade = path.name == "__init__.py"
    violations: list[Violation] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if is_facade:
            violations.extend(_check_facade(path, node))
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and not item.name.startswith("_"):
                violations.extend(_check_method(path, node, item))
    return violations


def main(argv: list[str] | None = None) -> int:
    """Entry point: check every mixin and facade file under ``pysisense/``.

    Parameters
    ----------
    argv : list[str] | None
        Optional explicit list of file paths to check. When omitted, every
        ``.py`` file under the package is scanned.

    Returns
    -------
    int
        ``0`` when no violations are found, ``1`` otherwise.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="Specific files to check (defaults to the whole package).")
    args = parser.parse_args(argv)

    if args.paths:
        files = [Path(p) for p in args.paths if p.endswith(".py") and Path(p).is_file()]
    else:
        files = sorted(PACKAGE_ROOT.rglob("*.py"))

    all_violations: list[Violation] = []
    for path in files:
        if path.name == "sisenseclient.py" or path.name == "utils.py":
            continue
        all_violations.extend(check_file(path))

    if all_violations:
        for v in sorted(all_violations, key=lambda x: (str(x.path), x.lineno)):
            print(v)
        counts: dict[str, int] = {}
        for v in all_violations:
            counts[v.code] = counts.get(v.code, 0) + 1
        summary = ", ".join(f"{code}={n}" for code, n in sorted(counts.items()))
        print(f"\n{len(all_violations)} docstring violation(s): {summary}", file=sys.stderr)
        return 1

    print("All docstring conventions satisfied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
