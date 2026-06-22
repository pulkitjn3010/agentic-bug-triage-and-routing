import asyncio
import json
import math
import os
import re
import time
import structlog
import httpx
from groq import AsyncGroq
from .base import BaseAgent
from ..connectors.registry import ConnectorRegistry

log = structlog.get_logger()

MAX_REACT_ITERS = 4

SYSTEM_PROMPT = """You are a technical documentation specialist in a strict ReAct loop.
Find the most relevant troubleshooting articles for the bug.

Tools:
Action: search_confluence
Action Input: <2-4 word query>

Action: search_stackoverflow
Action Input: <2-4 word query>

Rules:
- Use search_confluence for: config files, Apache docs, internal KB, deployment guides
- Use search_stackoverflow for: exceptions, code errors, runtime failures, JVM issues
- Use DEVELOPER vocabulary not formal descriptions
- Apache projects: use "apache-rat", "checkstyle", "rat plugin", "license header", "eslintrc"
- JVM issues: use class name + exception type
- Config issues: use exact filename
- If search returns nothing, try a different angle
- Maximum 4 searches total across both sources

Format:
Thought: <reasoning>
Action: search_confluence OR search_stackoverflow
Action Input: <query>

OR:
Final Answer: [{"title":"...","url":"...","excerpt":"...","relevance":"high|medium|low","source":"confluence|stackoverflow"}]

Always provide Final Answer even if empty."""


class EnrichmentAgent(BaseAgent):
    step_name = "enrichment"

    SOURCE_SPACE_MAP = {
        "apache-flink": "FLINK",
        "flink": "FLINK",
        "apache-spark": "SPARK",
        "spark": "SPARK",
        "kafka": "KAFKA",
        "apache-kafka": "KAFKA",
        "hpe": "HPEKB",
        "hpekb": "HPEKB",
        "hadoop": "HADOOP",
        "apache-hadoop": "HADOOP",
        "zookeeper": "ZOOKEEPER",
        "apache-zookeeper": "ZOOKEEPER",
    }
    DEFAULT_SPACE = "HPEKB"

    def _resolve_target_space(self, source_id: str) -> str:
        s = source_id.lower().rstrip("0123456789-")
        for key, space in self.SOURCE_SPACE_MAP.items():
            if s.startswith(key) or key in s:
                return space
        return self.DEFAULT_SPACE

    def _extract_initial_query(
        self, ticket_title: str, ticket_description: str, component: str = ""
    ) -> str:
        desc_snippet = (ticket_description or "")[:300]

        # Rule 1: file extension pattern
        file_pat = r"\.[\w]+(?:rc|config|yml|yaml|json|js|ts|xml|toml)"
        files = re.findall(file_pat, ticket_title)
        if files:
            return files[0][1:]
        files_desc = re.findall(file_pat, desc_snippet)
        if files_desc:
            return files_desc[0][1:]

        # Rule 2: CamelCase word
        camel_pat = r"\b[A-Z][a-z]+[A-Z][a-zA-Z]+\b"
        camel = re.findall(camel_pat, ticket_title)
        if camel:
            return camel[0]
        camel_desc = re.findall(camel_pat, desc_snippet)
        if camel_desc:
            return camel_desc[0]

        # Rule 3: Exception/Error/Failure suffix
        exc_pat = r"\b\w+(?:Exception|Error|Failure)\b"
        exc = re.findall(exc_pat, ticket_title)
        if exc:
            return exc[0]
        exc_desc = re.findall(exc_pat, desc_snippet)
        if exc_desc:
            return exc_desc[0]

        # Rule 4: fallback using component prefix
        comp_prefix = ""
        if component:
            parts = re.findall(r"\w+", component)
            if parts:
                comp_prefix = parts[0].lower() + " "

        words = [w.strip(".,()[]\"'") for w in ticket_title.split()]
        if comp_prefix:
            return comp_prefix + " ".join(words[:3])
        return " ".join(words[:4])



    async def run(self, context: dict) -> dict:
        primary = context.get("primary_ticket") or {}
        source_id = context.get("source_id", "")

        title = primary.get("title") or ""
        component = primary.get("component") or ""
        description = (primary.get("description") or "")[:400]
        error_excerpt = (primary.get("error_excerpt") or "")[:300]

        groq_api_key = os.getenv("GROQ_API_KEY", "")
        enrichment_model = "llama-3.1-8b-instant"

        # Determine correct Confluence space
        target_space = self._resolve_target_space(source_id)
        log.info("Enrichment target space", source_id=source_id, space=target_space)

        # Extract deterministic initial query
        initial_query = self._extract_initial_query(title, description, component)
        log.info("Enrichment initial query", query=initial_query)

        all_articles = await self._run_react_loop(
            title,
            component,
            description,
            error_excerpt,
            initial_query,
            target_space,
            groq_api_key,
            enrichment_model,
        )

        log.info(
            "Enrichment complete",
            total=len(all_articles),
        )

        context["kb_articles"] = all_articles[:6]
        context["enrichment_sources"] = all_articles
        return context



    async def _run_react_loop(
        self,
        title: str,
        component: str,
        description: str,
        error_excerpt: str,
        initial_query: str,
        target_space: str,
        api_key: str,
        model: str,
    ) -> list:
        if not api_key:
            return []

        client = AsyncGroq(api_key=api_key)
        seen_articles = []

        def add_to_accumulator(articles):
            for a in articles:
                if not any(x.get("url") == a.get("url") for x in seen_articles):
                    if "relevance" not in a:
                        a["relevance"] = "medium"
                    seen_articles.append(a)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Find articles for this bug:\n"
                    f"Title: {title}\n"
                    f"Component: {component}\n"
                    f"Description: {description}\n"
                    f"Error: {error_excerpt}\n\n"
                    f"Start with this search query: "
                    f"{initial_query}"
                ),
            },
        ]

        for iteration in range(MAX_REACT_ITERS):
            try:
                if iteration == 0:
                    query = initial_query
                    messages.append(
                        {
                            "role": "assistant",
                            "content": (
                                f"Thought: Starting with "
                                f"deterministic query.\n"
                                f"Action: search_confluence\n"
                                f"Action Input: {query}"
                            ),
                        }
                    )
                    conf_r, so_r = await asyncio.gather(
                        self._search_confluence(query, target_space),
                        self._fetch_stack_overflow(query),
                        return_exceptions=True,
                    )
                    if isinstance(conf_r, Exception):
                        log.warning(
                            "Confluence failed on iteration 0", error=str(conf_r)
                        )
                        conf_r = []
                    if isinstance(so_r, Exception):
                        log.warning(
                            "StackOverflow failed on iteration 0", error=str(so_r)
                        )
                        so_r = []

                    # Tag sources explicitly
                    for item in conf_r:
                        item["source"] = "confluence"
                    for item in so_r:
                        item["source"] = "stackoverflow"

                    add_to_accumulator(conf_r)
                    add_to_accumulator(so_r)

                    obs_lines = []
                    if conf_r:
                        obs_lines.append(f"Confluence ({target_space}): {json.dumps(conf_r)}")
                    else:
                        obs_lines.append(f"Confluence ({target_space}): No results for '{query}'. Try different terms.")
                    
                    if so_r:
                        obs_lines.append(f"StackOverflow: {json.dumps(so_r)}")
                    else:
                        obs_lines.append(f"StackOverflow: No results for '{query}'. Try different terms.")

                    obs = "\n".join(obs_lines)
                    messages.append(
                        {
                            "role": "user",
                            "content": f"Observation:\n{obs}",
                        }
                    )
                    continue

                resp = await client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=0.0,
                    max_tokens=512,
                )
                reply = resp.choices[0].message.content or ""
                messages.append({"role": "assistant", "content": reply})

                if "Final Answer:" in reply:
                    raw = reply.split("Final Answer:")[-1].strip()
                    raw = raw.strip("```json").strip("```").strip()
                    try:
                        parsed = json.loads(raw)
                        return parsed if isinstance(parsed, list) else []
                    except Exception:
                        return []

                if "Action: search_confluence" in reply and "Action Input:" in reply:
                    query = (
                        reply.split("Action Input:")[-1]
                        .strip()
                        .split("\n")[0]
                        .strip()
                        .strip("\"'")
                    )
                    results = await self._search_confluence(query, target_space)
                    add_to_accumulator(results)
                    if not results:
                        obs = f"Confluence: No results for '{query}' in {target_space} space. Try a different technical term or broader concept."
                    else:
                        obs = f"Confluence: {json.dumps(results)}"

                    messages.append(
                        {
                            "role": "user",
                            "content": f"Observation: {obs}",
                        }
                    )

                elif "Action: search_stackoverflow" in reply and "Action Input:" in reply:
                    query = (
                        reply.split("Action Input:")[-1]
                        .strip()
                        .split("\n")[0]
                        .strip()
                        .strip("\"'")
                    )
                    results = await self._fetch_stack_overflow(query)
                    add_to_accumulator(results)
                    if not results:
                        obs = f"StackOverflow: No results for '{query}'. Try a different technical term or broader concept."
                    else:
                        obs = f"StackOverflow: {json.dumps(results)}"

                    messages.append(
                        {
                            "role": "user",
                            "content": f"Observation: {obs}",
                        }
                    )

            except Exception as e:
                log.warning("ReAct iteration failed", error=str(e), iteration=iteration)
                break

        log.warning("LLM failed to output a Final Answer. Returning accumulated articles.")
        return seen_articles

    async def _search_confluence(
        self, query: str, target_space: str = None
    ) -> list[dict]:
        try:
            connectors = await ConnectorRegistry.get_all_by_type(
                "confluence"
            ) + await ConnectorRegistry.get_all_by_type("support_kb")
            if not connectors:
                try:
                    all_c = await ConnectorRegistry.get_all_enabled()
                    connectors = [
                        c for c in all_c if getattr(c, "is_knowledge_source", False)
                    ]
                except Exception:
                    return []

            if not connectors:
                return []

            # Find connector matching target space
            # Fall back to first confluence connector
            target_connector = None
            if target_space:
                for c in connectors:
                    if (c.project_key or "").upper() == (target_space.upper()):
                        target_connector = c
                        break
            if not target_connector:
                target_connector = connectors[0]

            results = await asyncio.wait_for(
                target_connector.search(query, max_results=5), timeout=15.0
            )

            output = []
            for t in results:
                article_text = t.description or ""
                score, chunks = self._slice_and_score(article_text, query, 0)
                excerpt = " ... ".join(chunks)[:400]
                relevance = "high" if score >= 0.25 else "medium" if score >= 0.10 else "low"
                output.append(
                    {
                        "title": t.title,
                        "url": t.url,
                        "excerpt": excerpt,
                        "relevance": relevance,
                        "source": target_connector.system_type,
                    }
                )
            log.info(
                "Confluence search", query=query, space=target_space, count=len(output)
            )
            return output

        except asyncio.TimeoutError:
            log.warning("Confluence timeout", query=query)
            return []
        except Exception as e:
            log.warning("Confluence error", error=str(e))
            return []

    async def _fetch_stack_overflow(
        self, query: str, max_results: int = 5
    ) -> list[dict]:
        try:
            url = "https://api.stackexchange.com/2.3/search/advanced"
            params = {
                "q": query,
                "site": "stackoverflow",
                "pagesize": max_results,
                "order": "desc",
                "sort": "relevance",
                "filter": "withbody",
            }
            async with httpx.AsyncClient(timeout=5, follow_redirects=True) as client:
                resp = await client.get(url, params=params)
                if resp.status_code != 200:
                    log.warning(
                        "StackOverflow advanced search failed",
                        status=resp.status_code,
                        query=query,
                    )
                    return []
                items = resp.json().get("items", [])
                results = []
                for item in items:
                    if item.get("answer_count", 0) < 1:
                        continue
                    body = item.get("body", "")
                    excerpt = re.sub(r"<[^>]+>", "", body)[:300]
                    score = item.get("score", 0)
                    answered = item.get("is_answered", False)
                    relevance = (
                        "high"
                        if answered and score > 5
                        else "medium" if answered else "low"
                    )
                    results.append(
                        {
                            "title": item.get("title", ""),
                            "url": item.get("link", ""),
                            "score": score,
                            "answer_count": item.get("answer_count", 0),
                            "excerpt": excerpt,
                            "relevance": relevance,
                            "source": "stackoverflow",
                        }
                    )
                log.info(
                    "StackOverflow advanced search", query=query, count=len(results)
                )
                return results[:max_results]
        except Exception as e:
            log.warning(
                "StackOverflow advanced search error", query=query, error=str(e)
            )
            return []

    def _slice_and_score(
        self, article_text: str, bug_text: str, last_modified_epoch: float = 0
    ) -> tuple[float, list[str]]:
        paragraphs = [
            p.strip() for p in article_text.split("\n\n") if len(p.strip()) > 30
        ]
        if not paragraphs:
            return 0.0, [article_text[:500]]

        if last_modified_epoch and last_modified_epoch > 0:
            delta = (time.time() - last_modified_epoch) / (365 * 24 * 3600)
            decay = math.exp(-0.15 * delta)
        else:
            decay = 0.9

        bug_words = set(bug_text.lower().split())
        scored = []
        for chunk in paragraphs:
            cwords = set(chunk.lower().split())
            if not cwords:
                continue
            overlap = len(bug_words & cwords) / len(bug_words | cwords)
            adjusted = overlap * decay
            has_fix = any(
                kw in chunk.lower()
                for kw in ("workaround", "patch", "fix", "resolution", "solution")
            )
            if adjusted >= 0.08 or has_fix:
                scored.append((adjusted, chunk))

        scored.sort(key=lambda x: x[0], reverse=True)
        top = [c for _, c in scored[:3]]
        top_score = scored[0][0] if scored else 0.0
        return top_score, (top if top else [article_text[:500]])
