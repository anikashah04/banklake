"""The user guide is complete and the committed docs/USER_GUIDE.md is in step with the code."""
from pathlib import Path

from banklake import guide

DOC = Path(__file__).resolve().parents[1] / "docs" / "USER_GUIDE.md"


def test_guide_markdown_contains_every_section_term_and_page():
    md = guide.to_markdown()
    for heading in ["Quick start", "Layout of the console", "Roles", "A five-minute demo", "Glossary", "Questions and problems"]:
        assert f"## {heading}" in md
    assert all(f"| {t} |" in md for t, _ in guide.GLOSSARY)
    assert all(v[0] in md for v in guide.PAGE_HELP.values())


def test_committed_user_guide_matches_generated_text():
    assert DOC.exists() and DOC.read_text() == guide.to_markdown(), "run: python main.py guide"


def test_roles_in_guide_match_real_permissions():
    from banklake import meta
    perms = {}
    for role, ent, col in meta.UNMASK:
        perms.setdefault(role, []).append(f"{ent}.{col}")
    assert perms["branch_manager"] == ["customer.customer_name"]
    assert sorted(perms["relationship_manager"]) == ["customer.customer_name", "customer.email", "customer.phone"]
    assert perms["compliance_officer"] == ["*.*"] and perms["admin"] == ["*.*"]
    assert "analyst" not in perms and "executive" not in perms
    assert any("branch_manager" in r and "names" in w for r, w in guide.ROLES)
