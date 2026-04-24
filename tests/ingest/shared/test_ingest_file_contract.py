from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from mind.commands import ingest


def test_ingest_file_routes_through_shared_lifecycle(monkeypatch, tmp_path: Path) -> None:
    sample = tmp_path / "sample.md"
    sample.write_text("# Sample\n\nhello world\n", encoding="utf-8")

    captured: dict[str, object] = {}

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)

        class Result:
            materialized = tmp_path / "raw" / "files" / "file-artifact-sample.md"

        return Result()

    monkeypatch.setattr(ingest, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)

    out = ingest.ingest_file(sample)

    assert out == tmp_path / "raw" / "files" / "file-artifact-sample.md"
    source = captured["source"]
    assert source.source_kind == "md"  # type: ignore[attr-defined]
    assert source.title == "Sample"  # type: ignore[attr-defined]
    assert captured["understand"] is ingest._understand_file_source
    assert captured["materialize"] is ingest._materialize_file_source
    assert captured["propagate"] is ingest._propagate_file_source


def test_materialize_file_source_preserves_absolute_path_outside_repo(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    fake_wiki = fake_root / "wiki"
    fake_raw = fake_root / "raw"
    fake_wiki.mkdir(parents=True)
    fake_raw.mkdir(parents=True)
    monkeypatch.setattr(
        ingest,
        "vault",
        lambda: SimpleNamespace(root=fake_root, wiki=fake_wiki, raw=fake_raw),
    )

    external = tmp_path / "outside.md"
    external.write_text("# External\n\nbody\n", encoding="utf-8")
    source = ingest._normalize_file_source(external.resolve())
    envelope = {"pass_a": {"excerpt": "body"}}

    materialized = ingest._materialize_file_source(source, envelope)
    text = materialized.read_text(encoding="utf-8")

    assert materialized == fake_raw / "files" / f"{source.source_id}.md"
    assert str(external.resolve()) in text
    assert f"source_path: {external.resolve()}" in text
