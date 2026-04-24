"""Provider-backed LLM capability seam for Brain."""
from __future__ import annotations

from typing import Any

from .llm_cache import LLMCacheIdentity
from .llm_executor import LLMExecutionResult, LLMExecutor
from .prompt_builders import (
    APPLIED_TO_POST_PROMPT_VERSION,
    APPLIED_TO_YOU_PROMPT_VERSION,
    CLASSIFY_BOOK_PROMPT_VERSION,
    CLASSIFY_LINKS_PROMPT_VERSION,
    CLASSIFY_VIDEO_PROMPT_VERSION,
    GENERATE_SKILL_PROMPT_VERSION,
    ONBOARDING_GRAPH_PROMPT_VERSION,
    ONBOARDING_GRAPH_CHUNK_PROMPT_VERSION,
    ONBOARDING_MATERIALIZATION_PROMPT_VERSION,
    ONBOARDING_MERGE_PROMPT_VERSION,
    ONBOARDING_MERGE_CHUNK_PROMPT_VERSION,
    ONBOARDING_MERGE_RELATIONSHIPS_PROMPT_VERSION,
    ONBOARDING_SYNTHESIS_PROMPT_VERSION,
    ONBOARDING_VERIFY_PROMPT_VERSION,
    RESEARCH_BOOK_DEEP_PROMPT_VERSION,
    RESEARCH_BOOK_PROMPT_VERSION,
    SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION,
    SUMMARIZE_ARTICLE_PROMPT_VERSION,
    SUMMARIZE_SUBSTACK_PROMPT_VERSION,
    SUMMARIZE_TRANSCRIPT_PROMPT_VERSION,
    TRANSCRIPT_CHAR_CAP,
    UPDATE_AUTHOR_STANCE_PROMPT_VERSION,
    align_classified_links,
    build_applied_to_post_prompt,
    build_applied_to_you_prompt,
    build_classify_book_prompt,
    build_classify_links_prompt,
    build_classify_video_prompt,
    build_generate_skill_prompt,
    build_onboarding_graph_prompt,
    build_onboarding_graph_chunk_prompt,
    build_onboarding_materialization_prompt,
    build_onboarding_merge_prompt,
    build_onboarding_merge_chunk_prompt,
    build_onboarding_merge_relationships_prompt,
    build_onboarding_synthesis_instructions,
    build_onboarding_verify_prompt,
    build_research_book_deep_prompt,
    build_research_book_prompt,
    build_summarize_book_research_prompt,
    build_summarize_article_prompt,
    build_summarize_substack_prompt,
    build_summarize_transcript_prompt,
    build_update_author_stance_prompt,
)
from .providers.base import (
    LLMConfigurationError,
    LLMInputPart,
    LLMServiceError,
    LLMStructuredOutputError,
    LLMTransportError,
)
from .llm_routing import TaskClass, resolve_route

TRANSCRIBE_YOUTUBE_PROMPT_VERSION = "youtube.transcription.v1"
SUMMARIZE_DOCUMENT_PROMPT_VERSION = "documents.summary.v1"
SUMMARIZE_BOOK_SOURCE_PROMPT_VERSION = "books.source-grounded.v1"
SUMMARIZE_BOOK_SOURCE_TEXT_PROMPT_VERSION = "books.source-grounded.text.v1"


def build_book_source_instructions(*, title: str, author: str, source_kind: str) -> str:
    if source_kind == "document":
        return (
            f"You are summarizing source-grounded book material for '{title}' by {author}. "
            "Return JSON with these exact keys: tldr, key_claims, frameworks_introduced, "
            "in_conversation_with, notable_quotes, topics. "
            "key_claims must be an array of objects with claim and evidence_context. "
            "Do not invent quotes beyond the provided source. Output JSON only."
        )
    return (
        f"You are transcribing and summarizing source-grounded audiobook material for '{title}' by {author}. "
        "Return JSON with these exact keys: transcript, tldr, key_claims, frameworks_introduced, "
        "in_conversation_with, notable_quotes, topics. "
        "transcript must contain the best faithful transcript you can recover from the provided audio. "
        "key_claims must be an array of objects with claim and evidence_context. "
        "Do not invent quotes beyond the provided source. Output JSON only."
    )


class LLMContractError(LLMServiceError):
    """Raised when a provider returns output that does not satisfy the contract."""


class LLMService:
    """Product-oriented LLM capability interface."""

    def __init__(self, *, executor: LLMExecutor | None = None):
        self.executor = executor or LLMExecutor()

    @property
    def runtime(self):
        return resolve_route("default")

    @property
    def provider_client(self):
        return self.executor._provider_client(  # noqa: SLF001 - compatibility surface for tests/debugging
            self.executor._build_prompt_request(  # noqa: SLF001 - compatibility surface for tests/debugging
                route=resolve_route("default"),
                task_class="default",
                prompt="",
                output_mode="text",
            )
        )

    def cache_identity(self, *, task_class: TaskClass, prompt_version: str) -> LLMCacheIdentity:
        return self.executor.cache_identity(task_class=task_class, prompt_version=prompt_version)

    def cache_identities(self, *, task_class: TaskClass, prompt_version: str) -> list[LLMCacheIdentity]:
        return self.executor.cache_identities(task_class=task_class, prompt_version=prompt_version)

    def cache_identities_for_parts(
        self,
        *,
        task_class: TaskClass,
        instructions: str,
        input_parts: list[LLMInputPart],
        prompt_version: str,
        input_mode: str,
        request_metadata: dict[str, Any] | None = None,
    ) -> list[LLMCacheIdentity]:
        return self.executor.cache_identities_for_parts(
            task_class=task_class,
            instructions=instructions,
            input_parts=input_parts,
            prompt_version=prompt_version,
            input_mode=input_mode,
            request_metadata=request_metadata,
        )

    def _generate_json(
        self,
        *,
        task_class: TaskClass,
        prompt: str,
        prompt_version: str,
        response_schema: dict[str, Any] | None = None,
    ) -> LLMExecutionResult:
        try:
            result = self.executor.execute_json(
                task_class=task_class,
                prompt=prompt,
                prompt_version=prompt_version,
                response_schema=response_schema,
            )
        except LLMConfigurationError as exc:
            raise LLMConfigurationError(str(exc)) from exc
        except LLMStructuredOutputError as exc:
            raise LLMContractError(str(exc)) from exc
        except LLMTransportError as exc:
            raise LLMServiceError(str(exc)) from exc
        return result

    def _generate_text(self, *, task_class: TaskClass, prompt: str, prompt_version: str) -> LLMExecutionResult:
        try:
            return self.executor.execute_text(task_class=task_class, prompt=prompt, prompt_version=prompt_version)
        except LLMConfigurationError as exc:
            raise LLMConfigurationError(str(exc)) from exc
        except LLMTransportError as exc:
            raise LLMServiceError(str(exc)) from exc

    def _generate_parts_json(
        self,
        *,
        task_class: TaskClass,
        instructions: str,
        input_parts: list[LLMInputPart],
        prompt_version: str,
        input_mode: str,
        request_metadata: dict[str, Any] | None = None,
        response_schema: dict[str, Any] | None = None,
    ) -> LLMExecutionResult:
        try:
            return self.executor.execute_parts_json(
                task_class=task_class,
                instructions=instructions,
                input_parts=input_parts,
                prompt_version=prompt_version,
                input_mode=input_mode,
                request_metadata=request_metadata,
                response_schema=response_schema,
            )
        except LLMConfigurationError as exc:
            raise LLMConfigurationError(str(exc)) from exc
        except LLMStructuredOutputError as exc:
            raise LLMContractError(str(exc)) from exc
        except LLMTransportError as exc:
            raise LLMServiceError(str(exc)) from exc

    def generate_json_prompt(
        self,
        prompt: str,
        *,
        with_meta: bool = False,
        task_class: TaskClass = "dream",
        prompt_version: str = "dream.generic-json.v1",
        response_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        """Run a generic contract-shaped JSON prompt through the provider seam."""
        result = self._generate_json(
            task_class=task_class,
            prompt=prompt,
            prompt_version=prompt_version,
            response_schema=response_schema,
        )
        data = result.data or {}
        identity = result.cache_identity
        return (data, identity) if with_meta else data

    def generate_parts_json_prompt(
        self,
        *,
        instructions: str,
        input_parts: list[LLMInputPart],
        with_meta: bool = False,
        task_class: TaskClass = "document",
        prompt_version: str = SUMMARIZE_DOCUMENT_PROMPT_VERSION,
        input_mode: str = "file",
        request_metadata: dict[str, Any] | None = None,
        response_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_parts_json(
            task_class=task_class,
            instructions=instructions,
            input_parts=input_parts,
            prompt_version=prompt_version,
            input_mode=input_mode,
            request_metadata=request_metadata,
            response_schema=response_schema,
        )
        data = result.data or {}
        return (data, result.cache_identity) if with_meta else data

    def classify_video(
        self,
        *,
        title: str,
        channel: str,
        description: str = "",
        tags: list[str] | None = None,
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="classification",
            prompt=build_classify_video_prompt(
                title=title,
                channel=channel,
                description=description,
                tags=tags,
            ),
            prompt_version=CLASSIFY_VIDEO_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="classification", prompt_version=CLASSIFY_VIDEO_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def classify_book(self, *, title: str, author: str, with_meta: bool = False) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="classification",
            prompt=build_classify_book_prompt(title=title, author=author),
            prompt_version=CLASSIFY_BOOK_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="classification", prompt_version=CLASSIFY_BOOK_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def summarize_transcript(self, *, title: str, channel: str, transcript: str, stance_context: str = "", prior_sources_context: str = "", with_meta: bool = False) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        prompt, truncated = build_summarize_transcript_prompt(
            title=title,
            channel=channel,
            transcript=transcript,
            stance_context=stance_context,
            prior_sources_context=prior_sources_context,
        )
        result = self._generate_json(task_class="summary", prompt=prompt, prompt_version=SUMMARIZE_TRANSCRIPT_PROMPT_VERSION)
        data = result.data or {}
        if truncated:
            data["truncated"] = True
            data["truncated_at_chars"] = TRANSCRIPT_CHAR_CAP
        identity = self.cache_identity(task_class="summary", prompt_version=SUMMARIZE_TRANSCRIPT_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def transcribe_youtube(
        self,
        *,
        title: str,
        channel: str,
        youtube_url: str,
        audio_bytes: bytes | None = None,
        audio_mime_type: str = "audio/mp4",
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        input_parts = [
            LLMInputPart.url_part(youtube_url, metadata={"source": "youtube"}),
            LLMInputPart.metadata_part({"title": title, "channel": channel}),
        ]
        if audio_bytes is not None:
            input_parts.append(
                LLMInputPart.audio_part(
                    audio_bytes,
                    mime_type=audio_mime_type,
                    file_name=f"{title[:40] or 'youtube-audio'}.{_audio_extension(audio_mime_type)}",
                    metadata={"youtube_url": youtube_url},
                )
            )
        result = self._generate_parts_json(
            task_class="transcription",
            instructions=(
                "Transcribe this YouTube source into JSON with these exact keys: "
                "transcript (string), summary (string), topics (array of lowercase-hyphenated strings), "
                "confidence (one of high|medium|low). Preserve speaker wording where possible. "
                "If only the URL is usable, infer from the accessible media or metadata rather than inventing details. "
                "Output JSON only."
            ),
            input_parts=input_parts,
            prompt_version=TRANSCRIBE_YOUTUBE_PROMPT_VERSION,
            input_mode="media",
            request_metadata={
                "youtube_url": youtube_url,
                "has_audio_bytes": audio_bytes is not None,
                "audio_mime_type": audio_mime_type if audio_bytes is not None else "",
            },
        )
        data = result.data or {}
        return (data, result.cache_identity) if with_meta else data

    def summarize_document(
        self,
        *,
        title: str,
        path_hint: str,
        document_bytes: bytes,
        mime_type: str = "application/pdf",
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_parts_json(
            task_class="document",
            instructions=(
                "Summarize this document into JSON with these exact keys: "
                "tldr (string), key_points (array of short strings), notable_quotes (array of strings), "
                "topics (array of lowercase-hyphenated strings), article (string). Output JSON only."
            ),
            input_parts=[
                LLMInputPart.metadata_part({"title": title, "path_hint": path_hint}),
                LLMInputPart.file_bytes_part(
                    document_bytes,
                    mime_type=mime_type,
                    file_name=path_hint.split("/")[-1] or "document.pdf",
                    kind="pdf_bytes" if mime_type == "application/pdf" else "file_bytes",
                ),
            ],
            prompt_version=SUMMARIZE_DOCUMENT_PROMPT_VERSION,
            input_mode="file",
            request_metadata={"path_hint": path_hint, "mime_type": mime_type},
        )
        data = result.data or {}
        return (data, result.cache_identity) if with_meta else data

    def summarize_book_source(
        self,
        *,
        title: str,
        author: str,
        input_parts: list[LLMInputPart],
        source_kind: str,
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        instructions = build_book_source_instructions(title=title, author=author, source_kind=source_kind)
        result = self._generate_parts_json(
            task_class="document" if source_kind == "document" else "transcription",
            instructions=instructions,
            input_parts=input_parts,
            prompt_version=SUMMARIZE_BOOK_SOURCE_PROMPT_VERSION,
            input_mode="file" if source_kind == "document" else "media",
            request_metadata={"title": title, "author": author, "source_kind": source_kind},
        )
        data = result.data or {}
        return (data, result.cache_identity) if with_meta else data

    def summarize_book_source_text(
        self,
        *,
        title: str,
        author: str,
        source_kind: str,
        excerpt: str,
        segment_label: str,
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="document" if source_kind == "document" else "transcription",
            prompt=(
                f"You are summarizing source-grounded {source_kind} material for '{title}' by {author}.\n"
                f"Excerpt label: {segment_label}\n\n"
                f"{excerpt}\n\n"
                + build_book_source_instructions(title=title, author=author, source_kind=source_kind)
            ),
            prompt_version=SUMMARIZE_BOOK_SOURCE_TEXT_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(
            task_class="document" if source_kind == "document" else "transcription",
            prompt_version=SUMMARIZE_BOOK_SOURCE_TEXT_PROMPT_VERSION,
        )
        return (data, identity) if with_meta else data

    def research_book(self, *, title: str, author: str, with_meta: bool = False) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="research",
            prompt=build_research_book_prompt(title=title, author=author),
            prompt_version=RESEARCH_BOOK_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="research", prompt_version=RESEARCH_BOOK_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def research_book_deep(self, *, title: str, author: str, with_meta: bool = False) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="research",
            prompt=build_research_book_deep_prompt(title=title, author=author),
            prompt_version=RESEARCH_BOOK_DEEP_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="research", prompt_version=RESEARCH_BOOK_DEEP_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def summarize_book_research(
        self,
        *,
        title: str,
        author: str,
        research: dict[str, Any],
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="summary",
            prompt=build_summarize_book_research_prompt(title=title, author=author, research=research),
            prompt_version=SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="summary", prompt_version=SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def applied_to_you(
        self,
        *,
        title: str,
        author: str,
        profile_context: str,
        research: dict[str, Any],
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="personalization",
            prompt=build_applied_to_you_prompt(
                title=title,
                author=author,
                profile_context=profile_context,
                research=research,
            ),
            prompt_version=APPLIED_TO_YOU_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="personalization", prompt_version=APPLIED_TO_YOU_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def summarize_substack_post(
        self,
        *,
        title: str,
        publication: str,
        author: str,
        body_markdown: str,
        prior_posts_context: str = "",
        stance_context: str = "",
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="summary",
            prompt=build_summarize_substack_prompt(
                title=title,
                publication=publication,
                author=author,
                body_markdown=body_markdown,
                prior_posts_context=prior_posts_context,
                stance_context=stance_context,
            ),
            prompt_version=SUMMARIZE_SUBSTACK_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="summary", prompt_version=SUMMARIZE_SUBSTACK_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def applied_to_post(
        self,
        *,
        title: str,
        publication: str,
        author: str,
        profile_context: str,
        summary: dict[str, Any],
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="personalization",
            prompt=build_applied_to_post_prompt(
                title=title,
                publication=publication,
                author=author,
                profile_context=profile_context,
                summary=summary,
            ),
            prompt_version=APPLIED_TO_POST_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="personalization", prompt_version=APPLIED_TO_POST_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def update_author_stance(
        self,
        *,
        author: str,
        title: str,
        post_slug: str,
        current_stance: str,
        summary: dict[str, Any],
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="stance",
            prompt=build_update_author_stance_prompt(
                author=author,
                title=title,
                post_slug=post_slug,
                current_stance=current_stance,
                summary=summary,
            ),
            prompt_version=UPDATE_AUTHOR_STANCE_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="stance", prompt_version=UPDATE_AUTHOR_STANCE_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    def classify_links_batch(
        self,
        *,
        post_title: str,
        publication: str,
        links: list[dict[str, str]],
        with_meta: bool = False,
    ) -> list[dict[str, str]] | tuple[list[dict[str, str]], LLMCacheIdentity]:
        if not links:
            return ([], self.cache_identity(task_class="classification", prompt_version=CLASSIFY_LINKS_PROMPT_VERSION)) if with_meta else []
        response = self._generate_json(
            task_class="classification",
            prompt=build_classify_links_prompt(
                post_title=post_title,
                publication=publication,
                links=links,
            ),
            prompt_version=CLASSIFY_LINKS_PROMPT_VERSION,
        )
        aligned = align_classified_links(links=links, response=response.data or {})
        identity = self.cache_identity(task_class="classification", prompt_version=CLASSIFY_LINKS_PROMPT_VERSION)
        return (aligned, identity) if with_meta else aligned

    def summarize_article(
        self,
        *,
        title: str,
        url: str,
        body_markdown: str,
        sitename: str | None,
        stance_context: str = "",
        prior_sources_context: str = "",
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        result = self._generate_json(
            task_class="summary",
            prompt=build_summarize_article_prompt(
                title=title,
                url=url,
                body_markdown=body_markdown,
                sitename=sitename,
                stance_context=stance_context,
                prior_sources_context=prior_sources_context,
            ),
            prompt_version=SUMMARIZE_ARTICLE_PROMPT_VERSION,
        )
        data = result.data or {}
        identity = self.cache_identity(task_class="summary", prompt_version=SUMMARIZE_ARTICLE_PROMPT_VERSION)
        return (data, identity) if with_meta else data

    # Alias used by scripts/articles/enrich.py
    summarize_article_text = summarize_article

    def generate_skill(
        self,
        *,
        task_description: str,
        context_text: str = "",
        task_class: TaskClass = "default",
        with_meta: bool = False,
    ) -> str | tuple[str, LLMCacheIdentity]:
        result = self._generate_text(
            task_class=task_class,
            prompt_version=GENERATE_SKILL_PROMPT_VERSION,
            prompt=build_generate_skill_prompt(
                task_description=task_description,
                context_text=context_text,
            )
        )
        text = result.text or ""
        identity = self.cache_identity(task_class=task_class, prompt_version=GENERATE_SKILL_PROMPT_VERSION)
        return (text, identity) if with_meta else text

    def synthesize_onboarding_semantics(
        self,
        *,
        bundle_id: str,
        input_parts: list[LLMInputPart],
        with_meta: bool = False,
        response_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        return self.generate_parts_json_prompt(
            instructions=build_onboarding_synthesis_instructions(bundle_id=bundle_id),
            input_parts=input_parts,
            with_meta=with_meta,
            task_class="onboarding_synthesis",
            prompt_version=ONBOARDING_SYNTHESIS_PROMPT_VERSION,
            input_mode="file",
            request_metadata={"bundle_id": bundle_id},
            response_schema=response_schema,
        )

    def shape_onboarding_graph(
        self,
        *,
        bundle: dict[str, Any],
        semantic_artifact: dict[str, Any],
        with_meta: bool = False,
        response_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        return self.generate_json_prompt(
            build_onboarding_graph_prompt(bundle=bundle, semantic_artifact=semantic_artifact),
            with_meta=with_meta,
            task_class="onboarding_synthesis",
            prompt_version=ONBOARDING_GRAPH_PROMPT_VERSION,
            response_schema=response_schema,
        )

    def merge_onboarding_graph(
        self,
        *,
        bundle: dict[str, Any],
        graph_artifact: dict[str, Any],
        candidate_context: dict[str, Any],
        with_meta: bool = False,
        response_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        return self.generate_json_prompt(
            build_onboarding_merge_prompt(
                bundle=bundle,
                graph_artifact=graph_artifact,
                candidate_context=candidate_context,
            ),
            with_meta=with_meta,
            task_class="onboarding_merge",
            prompt_version=ONBOARDING_MERGE_PROMPT_VERSION,
            response_schema=response_schema,
        )

    def shape_onboarding_graph_chunk(
        self,
        *,
        bundle: dict[str, Any],
        semantic_chunk: dict[str, Any],
        response_schema: dict[str, Any] | None = None,
    ) -> LLMExecutionResult:
        return self._generate_json(
            task_class="onboarding_synthesis",
            prompt=build_onboarding_graph_chunk_prompt(bundle=bundle, semantic_chunk=semantic_chunk),
            prompt_version=ONBOARDING_GRAPH_CHUNK_PROMPT_VERSION,
            response_schema=response_schema,
        )

    def merge_onboarding_graph_chunk(
        self,
        *,
        bundle: dict[str, Any],
        graph_chunk: dict[str, Any],
        response_schema: dict[str, Any] | None = None,
    ) -> LLMExecutionResult:
        return self._generate_json(
            task_class="onboarding_merge",
            prompt=build_onboarding_merge_chunk_prompt(bundle=bundle, graph_chunk=graph_chunk),
            prompt_version=ONBOARDING_MERGE_CHUNK_PROMPT_VERSION,
            response_schema=response_schema,
        )

    def merge_onboarding_relationships(
        self,
        *,
        bundle: dict[str, Any],
        kept_nodes: list[dict[str, Any]],
        edge_proposals: list[dict[str, Any]],
        response_schema: dict[str, Any] | None = None,
    ) -> LLMExecutionResult:
        return self._generate_json(
            task_class="onboarding_merge",
            prompt=build_onboarding_merge_relationships_prompt(
                bundle=bundle,
                kept_nodes=kept_nodes,
                edge_proposals=edge_proposals,
            ),
            prompt_version=ONBOARDING_MERGE_RELATIONSHIPS_PROMPT_VERSION,
            response_schema=response_schema,
        )

    def verify_onboarding_graph(
        self,
        *,
        bundle: dict[str, Any],
        semantic_artifact: dict[str, Any],
        graph_artifact: dict[str, Any],
        merge_artifact: dict[str, Any],
        with_meta: bool = False,
        response_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        return self.generate_json_prompt(
            build_onboarding_verify_prompt(
                bundle=bundle,
                semantic_artifact=semantic_artifact,
                graph_artifact=graph_artifact,
                merge_artifact=merge_artifact,
            ),
            with_meta=with_meta,
            task_class="onboarding_verify",
            prompt_version=ONBOARDING_VERIFY_PROMPT_VERSION,
            response_schema=response_schema,
        )

    def plan_onboarding_materialization(
        self,
        *,
        bundle: dict[str, Any],
        semantic_artifact: dict[str, Any],
        graph_artifact: dict[str, Any],
        merge_artifact: dict[str, Any],
        verify_artifact: dict[str, Any],
        with_meta: bool = False,
    ) -> dict[str, Any] | tuple[dict[str, Any], LLMCacheIdentity]:
        # Deterministic plan builder. Replaces the previous LLM call
        # which was prone to dropping entities, returning invalid
        # literals, and timing out. By the time merge decisions exist
        # producing a MaterializationPlan is mechanical, so the LLM is
        # redundant. Keep the public signature so call sites and mocks
        # are unaffected. See plans/lexical-plotting-fairy.md Phase A.
        from mind.services.onboarding_plan_builder import build_materialization_plan

        plan = build_materialization_plan(
            bundle_id=str(bundle.get("bundle_id") or ""),
            bundle=bundle,
            semantic=semantic_artifact,
            graph=graph_artifact,
            merge=merge_artifact,
            verify=verify_artifact,
        )
        identity = LLMCacheIdentity(
            task_class="onboarding_materialization",
            provider="deterministic",
            model="deterministic",
            transport="deterministic",
            api_family="deterministic",
            input_mode="text",
            prompt_version="onboarding.materialization-plan.deterministic-v1",
            request_fingerprint={"kind": "deterministic-builder"},
        )
        return (plan, identity) if with_meta else plan


def get_llm_service() -> LLMService:
    return LLMService()


def _audio_extension(mime_type: str) -> str:
    if mime_type == "audio/mpeg":
        return "mp3"
    if mime_type == "audio/wav":
        return "wav"
    if mime_type == "audio/mp4":
        return "m4a"
    if mime_type == "audio/webm":
        return "webm"
    return "bin"
