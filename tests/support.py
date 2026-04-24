from __future__ import annotations

from argparse import ArgumentParser, _SubParsersAction
import json
from pathlib import Path
from types import SimpleNamespace

from mind.services.llm_cache import LLMCacheIdentity
from scripts.common.slugify import slugify


def write_repo_config(
    root: Path,
    *,
    create_me: bool = False,
    create_indexes: bool = False,
    create_exports: bool = False,
    create_digests: bool = False,
    ingestors_enabled: list[str] | None = None,
    dream_enabled: bool | None = None,
) -> None:
    lines = [
        "vault:",
        "  wiki_dir: memory",
        "  raw_dir: raw",
        "  owner_profile: me/profile.md",
        "llm:",
        "  model: google/gemini-2.5-pro",
    ]
    if ingestors_enabled is not None:
        enabled = ", ".join(ingestors_enabled)
        lines.extend([
            "ingestors:",
            f"  enabled: [{enabled}]",
        ])
    if dream_enabled is not None:
        lines.extend([
            "dream:",
            f"  enabled: {'true' if dream_enabled else 'false'}",
        ])

    (root / "config.yaml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (root / "memory").mkdir(parents=True, exist_ok=True)
    (root / "raw").mkdir(parents=True, exist_ok=True)

    if create_me or create_digests:
        (root / "memory" / "me").mkdir(parents=True, exist_ok=True)
    if create_indexes:
        (root / "memory" / "CHANGELOG.md").write_text("# CHANGELOG\n", encoding="utf-8")
        (root / "memory" / "INDEX.md").write_text("# INDEX\n", encoding="utf-8")
    if create_exports:
        (root / "raw" / "exports").mkdir(parents=True, exist_ok=True)
    if create_digests:
        (root / "memory" / "me" / "digests").mkdir(parents=True, exist_ok=True)


def fake_env_config(root: Path, *, substack_session_cookie: str = "fake-cookie") -> SimpleNamespace:
    return SimpleNamespace(
        llm_model="google/gemini-2.5-pro",
        llm_routes={},
        llm_backup=None,
        ai_gateway_api_key="gateway",
        browser_for_cookies="chrome",
        repo_root=root,
        app_root=root,
        wiki_root=root / "memory",
        raw_root=root / "raw",
        substack_session_cookie=substack_session_cookie,
    )


def parser_for_command(parser: ArgumentParser, *command_path: str) -> ArgumentParser:
    current = parser
    for command in command_path:
        subparsers = next(
            (
                action
                for action in current._actions
                if isinstance(action, _SubParsersAction)
            ),
            None,
        )
        if subparsers is None or command not in subparsers.choices:
            raise AssertionError(f"missing command path: {' '.join(command_path)}")
        current = subparsers.choices[command]
    return current


def subcommand_names(parser: ArgumentParser, *command_path: str) -> set[str]:
    target = parser_for_command(parser, *command_path)
    subparsers = next(
        (
            action
            for action in target._actions
            if isinstance(action, _SubParsersAction)
        ),
        None,
    )
    return set(subparsers.choices) if subparsers is not None else set()


def option_strings(parser: ArgumentParser, *command_path: str) -> set[str]:
    target = parser_for_command(parser, *command_path)
    options: set[str] = set()
    for action in target._actions:
        options.update(action.option_strings)
    return options


class FakeOnboardingLLMService:
    def _identity(self, task_class: str, prompt_version: str) -> LLMCacheIdentity:
        return LLMCacheIdentity(
            task_class=task_class,
            provider="anthropic",
            model="anthropic/claude-sonnet-4.6",
            transport="ai_gateway",
            api_family="responses",
            input_mode="text",
            prompt_version=prompt_version,
            request_fingerprint={"kind": "test-double"},
        )

    def _bundle_from_parts(self, input_parts) -> dict[str, object]:
        for part in input_parts:
            text = getattr(part, "text", None)
            if not text or "Normalized onboarding evidence bundle JSON" not in text:
                continue
            _, _, payload = text.partition("\n\n")
            return json.loads(payload)
        raise AssertionError("normalized onboarding evidence bundle JSON part missing")

    def synthesize_onboarding_semantics(self, *, bundle_id: str, input_parts, with_meta: bool = False, response_schema=None):
        bundle = self._bundle_from_parts(input_parts)
        owner_slug = slugify(bundle["identity"]["name"])
        owner = {
            "name": bundle["identity"]["name"],
            "role": bundle["identity"].get("role", ""),
            "location": bundle["identity"].get("location", ""),
            "summary": bundle["identity"]["summary"],
            "values": list(bundle.get("values") or []),
            "positioning": dict(bundle.get("positioning") or {}),
            "open_inquiries": list(bundle.get("open_inquiries") or []),
        }
        entities = []
        for family in ("projects", "people", "concepts", "playbooks", "stances", "inquiries"):
            for item in bundle.get(family) or []:
                if family == "people" and item["slug"] == owner_slug:
                    continue
                entities.append(
                    {
                        "proposal_id": f"{family}:{item['slug']}",
                        "family": family,
                        "title": item["title"],
                        "slug": item["slug"],
                        "summary": item.get("summary", ""),
                        "domains": ["work"] if family == "projects" else ["meta"] if family != "people" else ["relationships"],
                        "aliases": [],
                        "evidence_refs": list(item.get("evidence_refs") or []),
                        "attributes": {
                            key: value
                            for key, value in item.items()
                            if key not in {"slug", "title", "summary", "evidence_refs"}
                        },
                    }
                )
        data = {
            "bundle_id": bundle_id,
            "owner": owner,
            "entities": entities,
            "relationships": [],
            "synthesis_notes": ["test synthesis"],
        }
        identity = self._identity("onboarding_synthesis", "onboarding.synthesis.semantic.v1")
        return (data, identity) if with_meta else data

    def shape_onboarding_graph(self, *, bundle, semantic_artifact, with_meta: bool = False, response_schema=None):
        page_types = {
            "projects": "project",
            "people": "person",
            "concepts": "concept",
            "playbooks": "playbook",
            "stances": "stance",
            "inquiries": "inquiry",
        }
        data = {
            "bundle_id": semantic_artifact["bundle_id"],
            "node_proposals": [
                {
                    "proposal_id": entity["proposal_id"],
                    "page_type": page_types[entity["family"]],
                    "slug": entity["slug"],
                    "title": entity["title"],
                    "summary": entity["summary"],
                    "domains": entity.get("domains") or ["meta"],
                    "aliases": entity.get("aliases") or [],
                    "evidence_refs": entity.get("evidence_refs") or [],
                    "attributes": entity.get("attributes") or {},
                    "relates_to_refs": [],
                }
                for entity in semantic_artifact.get("entities") or []
            ],
            "edge_proposals": [],
            "notes": ["test graph shaping"],
        }
        identity = self._identity("onboarding_synthesis", "onboarding.synthesis.graph.v1")
        return (data, identity) if with_meta else data

    def shape_onboarding_graph_chunk(self, *, bundle, semantic_chunk, response_schema=None):
        page_types = {
            "projects": "project",
            "people": "person",
            "concepts": "concept",
            "playbooks": "playbook",
            "stances": "stance",
            "inquiries": "inquiry",
        }
        return {
            "bundle_id": semantic_chunk["bundle_id"],
            "node_proposals": [
                {
                    "proposal_id": entity["proposal_id"],
                    "page_type": page_types[entity["family"]],
                    "slug": entity["slug"],
                    "title": entity["title"],
                    "summary": entity["summary"],
                    "domains": entity.get("domains") or ["meta"],
                    "aliases": entity.get("aliases") or [],
                    "evidence_refs": entity.get("evidence_refs") or [],
                    "attributes": entity.get("attributes") or {},
                    "relates_to_refs": [],
                }
                for entity in semantic_chunk.get("entities") or []
            ],
            "edge_proposals": [
                {
                    "source_ref": rel["source_ref"],
                    "target_ref": rel["target_ref"],
                    "relation_type": rel["relation_type"],
                    "rationale": rel["rationale"],
                    "evidence_refs": rel.get("evidence_refs") or [],
                }
                for rel in semantic_chunk.get("relationships") or []
            ],
            "notes": ["test graph chunk"],
        }

    def merge_onboarding_graph(self, *, bundle, graph_artifact, candidate_context, with_meta: bool = False, response_schema=None):
        decisions = []
        owner_slug = slugify(bundle["identity"]["name"])
        for node in graph_artifact.get("node_proposals") or []:
            if node["page_type"] == "person" and node["slug"] == owner_slug:
                decisions.append(
                    {
                        "proposal_id": node["proposal_id"],
                        "source_proposal_id": node["proposal_id"],
                        "action": "merge",
                        "title": node["title"],
                        "slug": node["slug"],
                        "summary": node["summary"],
                        "page_type": node["page_type"],
                        "domains": list(node.get("domains") or []),
                        "relates_to": list(node.get("relates_to_refs") or []),
                        "target_page_id": f"{owner_slug}-person",
                        "target_page_type": "person",
                        "target_path": f"memory/people/{owner_slug}-person.md",
                        "rationale": "Owner person proposal merges into the canonical owner node.",
                        "evidence_refs": list(node.get("evidence_refs") or []),
                    }
                )
                continue
            candidates = next((item for item in candidate_context.get("candidates") or [] if item.get("proposal_id") == node["proposal_id"]), {})
            exact = list(candidates.get("exact_candidates") or [])
            if exact:
                target = exact[0]
                decisions.append(
                    {
                        "proposal_id": node["proposal_id"],
                        "source_proposal_id": node["proposal_id"],
                        "action": "merge",
                        "title": node["title"],
                        "slug": node["slug"],
                        "summary": node["summary"],
                        "page_type": node["page_type"],
                        "domains": list(node.get("domains") or []),
                        "relates_to": list(node.get("relates_to_refs") or []),
                        "target_page_id": target["page_id"],
                        "target_page_type": target["primary_type"],
                        "target_path": target["path"],
                        "rationale": "Matched existing node in test double.",
                        "evidence_refs": list(node.get("evidence_refs") or []),
                    }
                )
            else:
                decisions.append(
                    {
                        "proposal_id": node["proposal_id"],
                        "source_proposal_id": node["proposal_id"],
                        "action": "create",
                        "title": node["title"],
                        "slug": node["slug"],
                        "summary": node["summary"],
                        "page_type": node["page_type"],
                        "domains": list(node.get("domains") or []),
                        "relates_to": list(node.get("relates_to_refs") or []),
                        "target_page_id": None,
                        "target_page_type": None,
                        "target_path": None,
                        "rationale": "No existing match in test double.",
                        "evidence_refs": list(node.get("evidence_refs") or []),
                    }
                )
        data = {
            "bundle_id": graph_artifact["bundle_id"],
            "decisions": decisions,
            "relationship_decisions": [],
            "notes": ["test merge"],
        }
        identity = self._identity("onboarding_merge", "onboarding.merge.v1")
        return (data, identity) if with_meta else data

    def merge_onboarding_graph_chunk(self, *, bundle, graph_chunk, response_schema=None):
        decisions = []
        owner_slug = slugify(bundle["identity"]["name"])
        candidates_by_id = {str(item.get("proposal_id") or ""): item for item in graph_chunk.get("candidates") or []}
        for node in graph_chunk.get("node_proposals") or []:
            if node["page_type"] == "person" and node["slug"] == owner_slug:
                decisions.append(
                    {
                        "proposal_id": node["proposal_id"],
                        "source_proposal_id": node["proposal_id"],
                        "action": "merge",
                        "title": node["title"],
                        "slug": node["slug"],
                        "summary": node["summary"],
                        "page_type": node["page_type"],
                        "domains": list(node.get("domains") or []),
                        "relates_to": list(node.get("relates_to_refs") or []),
                        "target_page_id": f"{owner_slug}-person",
                        "target_page_type": "person",
                        "target_path": f"memory/people/{owner_slug}-person.md",
                        "rationale": "Owner person proposal merges into the canonical owner node.",
                        "evidence_refs": list(node.get("evidence_refs") or []),
                    }
                )
                continue
            exact = list((candidates_by_id.get(node["proposal_id"]) or {}).get("exact_candidates") or [])
            if exact:
                target = exact[0]
                decisions.append(
                    {
                        "proposal_id": node["proposal_id"],
                        "source_proposal_id": node["proposal_id"],
                        "action": "merge",
                        "title": node["title"],
                        "slug": node["slug"],
                        "summary": node["summary"],
                        "page_type": node["page_type"],
                        "domains": list(node.get("domains") or []),
                        "relates_to": list(node.get("relates_to_refs") or []),
                        "target_page_id": target["page_id"],
                        "target_page_type": target["primary_type"],
                        "target_path": target["path"],
                        "rationale": "Matched existing node in test double.",
                        "evidence_refs": list(node.get("evidence_refs") or []),
                    }
                )
            else:
                decisions.append(
                    {
                        "proposal_id": node["proposal_id"],
                        "source_proposal_id": node["proposal_id"],
                        "action": "create",
                        "title": node["title"],
                        "slug": node["slug"],
                        "summary": node["summary"],
                        "page_type": node["page_type"],
                        "domains": list(node.get("domains") or []),
                        "relates_to": list(node.get("relates_to_refs") or []),
                        "target_page_id": None,
                        "target_page_type": None,
                        "target_path": None,
                        "rationale": "No existing match in test double.",
                        "evidence_refs": list(node.get("evidence_refs") or []),
                    }
                )
        return {
            "bundle_id": graph_chunk["bundle_id"],
            "decisions": decisions,
            "notes": ["test merge chunk"],
        }

    def merge_onboarding_relationships(self, *, bundle, kept_nodes, edge_proposals, response_schema=None):
        return {
            "bundle_id": bundle["bundle_id"],
            "relationship_decisions": [
                {
                    "source_ref": edge["source_ref"],
                    "target_ref": edge["target_ref"],
                    "action": "keep",
                    "rationale": "Keep grounded relationship in test double.",
                    "evidence_refs": list(edge.get("evidence_refs") or []),
                }
                for edge in edge_proposals
            ],
            "notes": ["test merge relationships"],
        }

    def verify_onboarding_graph(self, *, bundle, semantic_artifact, graph_artifact, merge_artifact, with_meta: bool = False, response_schema=None):
        data = {
            "bundle_id": bundle["bundle_id"],
            "approved": True,
            "blocking_issues": [],
            "warnings": [],
            "notes": ["test verify"],
        }
        identity = self._identity("onboarding_verify", "onboarding.verify.v1")
        return (data, identity) if with_meta else data

    def plan_onboarding_materialization(self, *, bundle, semantic_artifact, graph_artifact, merge_artifact, verify_artifact, with_meta: bool = False):
        bundle_id = bundle["bundle_id"]
        owner = semantic_artifact["owner"]
        owner_slug = slugify(owner["name"])
        overview_id = f"summary-onboarding-{bundle_id}-overview"
        source_links = [f"[[{overview_id}]]"]
        pages = [
            {
                "plan_id": "owner-profile",
                "target_kind": "owner_profile",
                "write_mode": "create",
                "page_type": "profile",
                "slug": "profile",
                "title": owner["name"],
                "body_markdown": f"# {owner['name']}\n\n{owner['summary']}\n",
                "intro_mode": "preserve",
                "intro_markdown": None,
                "section_operations": [],
                "domains": ["identity", "work"],
                "relates_to": [f"[[{slugify(owner['name'])}]]"],
                "sources": source_links,
                "extra_frontmatter": {"role": owner.get("role", ""), "location": owner.get("location", "")},
                "target_path": None,
                "summary_kind": None,
            },
            {
                "plan_id": "owner-values",
                "target_kind": "owner_values",
                "write_mode": "create",
                "page_type": "note",
                "slug": "values",
                "title": "Values",
                "body_markdown": "# Values\n\n" + "\n".join(f"- {item['text']}" for item in owner.get("values") or []) + "\n",
                "intro_mode": "preserve",
                "intro_markdown": None,
                "section_operations": [],
                "domains": ["identity", "craft"],
                "relates_to": ["[[profile]]"],
                "sources": source_links,
                "extra_frontmatter": {},
                "target_path": None,
                "summary_kind": None,
            },
            {
                "plan_id": "owner-positioning",
                "target_kind": "owner_positioning",
                "write_mode": "create",
                "page_type": "note",
                "slug": "positioning",
                "title": "Positioning",
                "body_markdown": "# Positioning\n\n" + owner["positioning"]["summary"] + "\n",
                "intro_mode": "preserve",
                "intro_markdown": None,
                "section_operations": [],
                "domains": ["work", "identity"],
                "relates_to": ["[[profile]]"],
                "sources": source_links,
                "extra_frontmatter": {},
                "target_path": None,
                "summary_kind": None,
            },
            {
                "plan_id": "owner-open-inquiries",
                "target_kind": "owner_open_inquiries",
                "write_mode": "create",
                "page_type": "note",
                "slug": "open-inquiries",
                "title": "Open Inquiries",
                "body_markdown": "# Open Inquiries\n\n" + "\n".join(f"- [[{item['slug']}]]" for item in owner.get("open_inquiries") or []) + "\n",
                "intro_mode": "preserve",
                "intro_markdown": None,
                "section_operations": [],
                "domains": ["meta"],
                "relates_to": [],
                "sources": source_links,
                "extra_frontmatter": {},
                "target_path": None,
                "summary_kind": None,
            },
            {
                "plan_id": "owner-person",
                "target_kind": "owner_person",
                "write_mode": "create",
                "page_type": "person",
                "slug": owner_slug,
                "title": owner["name"],
                "body_markdown": f"# {owner['name']}\n\n{owner['summary']}\n",
                "intro_mode": "preserve",
                "intro_markdown": None,
                "section_operations": [],
                "domains": ["identity", "relationships"],
                "relates_to": ["[[profile]]"],
                "sources": source_links,
                "extra_frontmatter": {},
                "target_path": None,
                "summary_kind": None,
            },
        ]
        for kind, title, body in (
            ("overview", f"Onboarding Overview {bundle_id}", f"# Summary — Overview\n\n{owner['summary']}\n"),
            ("profile", f"Onboarding Profile Summary {bundle_id}", f"# Summary — Profile\n\n{owner['summary']}\n"),
            ("values", f"Onboarding Values Summary {bundle_id}", "# Summary — Values\n\n" + "\n".join(f"- {item['text']}" for item in owner.get("values") or []) + "\n"),
            ("positioning", f"Onboarding Positioning Summary {bundle_id}", f"# Summary — Positioning\n\n{owner['positioning']['summary']}\n"),
            ("open-inquiries", f"Onboarding Open Inquiries Summary {bundle_id}", "# Summary — Open Inquiries\n\n" + "\n".join(f"- {item['question']}" for item in owner.get("open_inquiries") or []) + "\n"),
        ):
            pages.append(
                {
                    "plan_id": f"summary-{kind}",
                    "target_kind": "summary",
                    "write_mode": "create",
                    "page_type": "summary",
                    "slug": f"summary-onboarding-{bundle_id}-{kind}",
                    "title": title,
                    "body_markdown": body,
                    "intro_mode": "preserve",
                    "intro_markdown": None,
                    "section_operations": [],
                    "domains": ["meta"],
                    "relates_to": ["[[profile]]"],
                    "sources": [],
                    "extra_frontmatter": {
                        "source_type": "onboarding",
                        "source_date": "2026-04-10",
                        "ingested": "2026-04-10",
                        "external_id": bundle_id,
                        "source_path": f"raw/onboarding/bundles/{bundle_id}/normalized-evidence.json",
                    },
                    "target_path": None,
                    "summary_kind": kind,
                }
            )
        for node in graph_artifact.get("node_proposals") or []:
            decision = next(item for item in merge_artifact.get("decisions") or [] if item["proposal_id"] == node["proposal_id"])
            if node["page_type"] == "person" and node["slug"] == owner_slug:
                continue
            pages.append(
                {
                    "plan_id": node["proposal_id"],
                    "target_kind": "canonical",
                    "write_mode": "update" if decision["action"] in {"update", "merge"} else "create",
                    "page_type": node["page_type"],
                    "slug": node["slug"],
                    "title": node["title"],
                    "body_markdown": None if decision["action"] in {"update", "merge"} else f"# {node['title']}\n\n{node['summary']}\n",
                    "intro_mode": "replace" if decision["action"] in {"update", "merge"} else "preserve",
                    "intro_markdown": f"# {node['title']}\n\n{node['summary']}\n" if decision["action"] in {"update", "merge"} else None,
                    "section_operations": [],
                    "domains": list(node.get("domains") or ["meta"]),
                    "relates_to": [],
                    "sources": source_links,
                    "extra_frontmatter": dict(node.get("attributes") or {}),
                    "target_path": decision.get("target_path"),
                    "summary_kind": None,
                }
            )
        pages.append(
            {
                "plan_id": "decision-page",
                "target_kind": "decision",
                "write_mode": "create",
                "page_type": "decision",
                "slug": f"onboarding-{bundle_id}",
                "title": f"Onboarding decisions {bundle_id}",
                "body_markdown": "# Onboarding Decisions\n\n"
                + "\n".join(
                    f"## {item['proposal_id']}\n\n- action: {item['action']}\n- rationale: {item['rationale']}\n"
                    for item in merge_artifact.get("decisions") or []
                )
                + "\n",
                "intro_mode": "preserve",
                "intro_markdown": None,
                "section_operations": [],
                "domains": ["work", "meta"],
                "relates_to": ["[[profile]]"],
                "sources": source_links,
                "extra_frontmatter": {},
                "target_path": None,
                "summary_kind": None,
            }
        )
        data = {"bundle_id": bundle_id, "pages": pages, "notes": ["test materialization plan"]}
        identity = self._identity("onboarding_verify", "onboarding.materialization-plan.v1")
        return (data, identity) if with_meta else data


def patch_onboarding_llm(monkeypatch) -> FakeOnboardingLLMService:
    service = FakeOnboardingLLMService()
    monkeypatch.setattr("mind.services.onboarding_synthesis.get_llm_service", lambda: service)
    return service
