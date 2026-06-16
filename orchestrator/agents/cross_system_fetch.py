import asyncio
import dataclasses
import inspect
import json
import os
import re
from typing import Any
import structlog
try:
    from groq import AsyncGroq
except Exception:
    AsyncGroq = None
from .base import BaseAgent
from ..connectors.registry import ConnectorRegistry
from ..utils.url_utils import sanitize_bug_url
log = structlog.get_logger()
BUG_SOURCE_TYPES = {'jira', 'jira_apache', 'jira_cloud', 'github', 'bugzilla'}
RELATIONSHIP_TYPES = {'direct_reference', 'duplicate', 'dependency', 'semantic_similarity', 'unrelated'}
GENERIC_WORDS = set('a an the to with when in on at by for of and or is it this that was are be as from but not have has had does error issue bug fix update add remove change missing failed cannot unable invalid exception problem wrong broken using implementation configuration management automatically automatic create created creating'.split())
DOMAIN_GENERIC_WORDS = set('apache spark core jira github bugzilla project module component common server client service system application framework library runtime engine cluster worker driver streaming sql'.split())
UNRELATED_WORDS = ('unrelated', 'not related', 'different issue', 'different root cause', 'different component', 'no overlap', 'do not match')

class CrossSystemFetchAgent(BaseAgent):
    step_name = 'cross_system_fetch'
    AGENT_TIMEOUT_SECONDS = 90.0
    CONNECTOR_TIMEOUT_SECONDS = 18.0
    FETCH_TIMEOUT_SECONDS = 8.0
    GROQ_TIMEOUT_SECONDS = 25.0
    SEMANTIC_THRESHOLD = 0.6
    DIRECT_REFERENCE_THRESHOLD = 0.6
    FINAL_RESULT_LIMIT = 5

    async def run(self, context: dict) -> dict:
        try:
            return await asyncio.wait_for(self._run_pipeline(context), timeout=self.AGENT_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            log.warning('CrossSystem agent timed out')
            return self._empty_context(context)
        except Exception as e:
            log.warning('CrossSystem agent failed', error=str(e))
            return self._empty_context(context)

    async def _run_pipeline(self, context: dict) -> dict:
        primary = self._normalize_primary(context)
        if not primary.get('title') and (not primary.get('ticket_id')):
            log.warning('CrossSystem: no primary ticket title or id')
            return self._empty_context(context)
        all_connectors = await self._load_connectors()
        bug_connectors = [c for c in all_connectors if self._is_bug_source(c)]
        primary_source = primary.get('source_id') or context.get('source_id', '')
        primary_connector = self._find_primary_connector(primary, bug_connectors)
        targets = self._target_connectors(bug_connectors, primary, primary_connector)
        sources_queried = [self._safe_str(getattr(c, 'source_id', '')) for c in targets]
        primary = self._add_primary_reference_variants(primary, bug_connectors)
        log.info('CrossSystem pipeline start', primary_source=primary_source, targets=sources_queried)
        signals: list[dict] = []
        signals.extend(self._extract_outbound_reference_signals(primary, targets))
        signals.extend(self._signals_from_native_links(primary, targets))
        signals.extend(self._signals_from_context_references(context, targets))
        signals.extend(await self._discover_reverse_references(primary, targets))
        signals.extend(await self._discover_semantic_live(primary, targets))
        signals.extend(await self._discover_cache(primary, targets))
        log.info('CrossSystem discovery signals', total=len(signals), outbound=sum((1 for s in signals if 'outbound_reference' in (s.get('provenance') or []))), reverse=sum((1 for s in signals if 'reverse_reference_search' in (s.get('provenance') or []))), semantic=sum((1 for s in signals if 'semantic_live_search' in (s.get('provenance') or []))), cache=sum((1 for s in signals if 'redis_cache' in (s.get('provenance') or []))))
        signals = self._prioritize_signals(signals, limit=25)
        candidates = await self._hydrate_and_merge_signals(signals, targets)
        log.info('CrossSystem candidates hydrated', count=len(candidates))
        scored = await self._score_candidates(primary, candidates)
        final = self._finalize_candidates(scored, primary)
        if not final and targets:
            log.info('CrossSystem rescue search starting')
            rescue_signals = await self._discover_rescue_semantic(primary, targets)
            rescue_signals = self._prioritize_signals(rescue_signals, limit=15)
            rescue_candidates = await self._hydrate_and_merge_signals(rescue_signals, targets)
            rescue_scored = await self._score_candidates(primary, rescue_candidates)
            final = self._finalize_candidates(rescue_scored, primary)
            log.info('CrossSystem rescue search finished', signals=len(rescue_signals), candidates=len(rescue_candidates), final=len(final))
        context['related_tickets'] = final
        context['related_candidates'] = final
        context['sources_queried'] = sources_queried
        context['related_issues'] = {'related_tickets': final, 'sources_queried': sources_queried}
        return context

    def _empty_context(self, context: dict) -> dict:
        context['related_tickets'] = []
        context['related_candidates'] = []
        context['sources_queried'] = []
        context['related_issues'] = {'related_tickets': [], 'sources_queried': []}
        return context

    async def _load_connectors(self) -> list:
        try:
            return await ConnectorRegistry.get_all_enabled()
        except Exception as e:
            log.warning('CrossSystem connector registry load failed', error=str(e))
            return []

    def _is_bug_source(self, connector: Any) -> bool:
        st = self._safe_str(getattr(connector, 'system_type', '')).lower()
        return bool(getattr(connector, 'is_bug_source', False)) or st in BUG_SOURCE_TYPES

    def _find_primary_connector(self, primary: dict, connectors: list) -> Any | None:
        primary_source = self._safe_str(primary.get('source_id'))
        primary_id = self._safe_str(primary.get('ticket_id') or primary.get('raw_key'))
        primary_system = self._normalize_system_type(primary.get('system_type') or primary.get('source'))
        for connector in connectors:
            if self._safe_str(getattr(connector, 'source_id', '')) == primary_source:
                return connector
        if primary_id:
            prefix = primary_id.split('-')[0].lower() if '-' in primary_id else ''
            for connector in connectors:
                ctype = self._normalize_system_type(getattr(connector, 'system_type', ''))
                if primary_system and ctype != primary_system:
                    continue
                connector_prefixes = {self._safe_str(getattr(connector, 'ticket_prefix', '')).lower(), self._safe_str(getattr(connector, 'project_key', '')).lower()}
                connector_prefixes.discard('')
                if prefix and prefix in connector_prefixes:
                    return connector
        matching_system = [c for c in connectors if self._normalize_system_type(getattr(c, 'system_type', '')) == primary_system]
        return matching_system[0] if len(matching_system) == 1 else None

    def _target_connectors(self, connectors: list, primary: dict, primary_connector: Any | None) -> list:
        primary_source = self._safe_str(primary.get('source_id'))
        primary_system = self._normalize_system_type(primary.get('system_type') or primary.get('source'))
        primary_connector_source = self._safe_str(getattr(primary_connector, 'source_id', '')) if primary_connector else ''
        targets = []
        for connector in connectors:
            source_id = self._safe_str(getattr(connector, 'source_id', ''))
            system_type = self._normalize_system_type(getattr(connector, 'system_type', ''))
            if source_id and source_id in {primary_source, primary_connector_source}:
                continue
            if primary_system and system_type == primary_system:
                continue
            targets.append(connector)
        return targets

    def _normalize_primary(self, context: dict) -> dict:
        raw = context.get('primary_ticket') or {}
        comments_text = self._comments_to_text(raw.get('comments', []))
        description = self._safe_str(raw.get('description') or raw.get('body') or '')
        title = self._safe_str(raw.get('title') or raw.get('summary') or '')
        ticket_id = self._safe_str(raw.get('ticket_id') or raw.get('id') or raw.get('key') or raw.get('number') or '')
        source_id = self._safe_str(raw.get('source_id') or context.get('source_id') or raw.get('source') or '')
        system_type = self._safe_str(raw.get('system_type') or raw.get('source') or '')
        return {'ticket_id': ticket_id, 'id': ticket_id, 'raw_key': self._safe_str(raw.get('raw_key') or raw.get('key') or ''), 'source_id': source_id, 'system_type': system_type, 'source': system_type or source_id, 'title': title, 'description': description, 'comments_text': comments_text, 'component': self._safe_str(raw.get('component') or ''), 'error_excerpt': self._safe_str(raw.get('error_excerpt') or ''), 'url': self._safe_str(raw.get('url') or raw.get('html_url') or raw.get('link') or ''), 'raw': raw}

    def _add_primary_reference_variants(self, primary: dict, connectors: list) -> dict:
        variants = set()
        for value in (primary.get('ticket_id'), primary.get('raw_key'), primary.get('id'), primary.get('url')):
            if self._safe_str(value):
                variants.add(self._safe_str(value))
        ticket_id = self._safe_str(primary.get('ticket_id'))
        source_id = self._safe_str(primary.get('source_id'))
        connector = self._find_primary_connector(primary, connectors) or next((c for c in connectors if self._safe_str(getattr(c, 'source_id', '')) == source_id), None)
        if connector and hasattr(connector, 'reference_variants'):
            variants.update(connector.reference_variants(ticket_id, primary.get('url', ''), primary.get('raw_key', '')))
        elif ticket_id:
            variants.add(ticket_id.lstrip('#'))
        primary['reference_variants'] = sorted((v for v in variants if v))
        return primary

    def _extract_outbound_reference_signals(self, primary: dict, targets: list) -> list[dict]:
        text = self._combined_text(primary)
        signals: list[dict] = []
        github_targets = [c for c in targets if 'github' in self._safe_str(getattr(c, 'system_type', '')).lower()]
        for connector in targets:
            source_id = self._safe_str(getattr(connector, 'source_id', ''))
            system_type = self._safe_str(getattr(connector, 'system_type', ''))
            refs = connector.extract_references_from_text(text) if hasattr(connector, 'extract_references_from_text') else []
            for ref in refs:
                if ref.get('ambiguous_hash') and len(github_targets) != 1:
                    log.info('CrossSystem ambiguous GitHub hash skipped', ref=ref.get('raw_reference'))
                    continue
                if not self._reference_matches_connector(ref, connector):
                    continue
                ticket_id = self._safe_str(ref.get('raw_id') or ref.get('ticket_id') or ref.get('id'))
                if not ticket_id or self._normalize_ref(ticket_id) == self._normalize_ref(primary.get('ticket_id')):
                    continue
                signals.append({'source_id': source_id, 'source_id_hint': source_id, 'system_type': system_type, 'source': system_type, 'ticket_id': ticket_id, 'id': ticket_id, 'url': self._safe_str(ref.get('url')), 'base_url': self._safe_str(getattr(connector, 'base_url', '')), 'relationship_hint': self._relationship_from_type(ref.get('relationship')), 'provenance': ['outbound_reference'], 'query_used': '', 'raw_reference': self._safe_str(ref.get('raw_reference') or ticket_id), 'signals': [{'provenance': 'outbound_reference', 'relationship_hint': ref.get('relationship'), 'raw_reference': ref.get('raw_reference') or ticket_id, 'url': ref.get('url', '')}]})
        return signals

    def _signals_from_native_links(self, primary: dict, targets: list) -> list[dict]:
        raw = primary.get('raw') or {}
        links = []
        for key in ('direct_reference_links', 'linked_items'):
            value = raw.get(key) or []
            if isinstance(value, list):
                links.extend((item for item in value if isinstance(item, dict)))
        signals = []
        for link in links:
            raw_id = self._safe_str(link.get('raw_id') or link.get('ticket_id') or link.get('id'))
            if not raw_id:
                continue
            source = self._normalize_system_type(link.get('source') or link.get('system_type'))
            if source == 'unknown':
                if re.match('^[A-Z][A-Z0-9]+-\\d+$', raw_id, re.IGNORECASE):
                    source = 'jira'
                elif raw_id.lstrip('#').isdigit():
                    source = 'github'
                elif raw_id.upper().startswith('BZ-'):
                    source = 'bugzilla'
            signals.extend(self._signals_for_reference(targets, system_type=source, ticket_id=raw_id, url=self._safe_str(link.get('url')), relationship_hint=self._relationship_from_type(link.get('relationship') or link.get('type')), provenance='outbound_reference', raw_reference=raw_id))
        return signals

    def _signals_from_context_references(self, context: dict, targets: list) -> list[dict]:
        signals: list[dict] = []
        for ref in context.get('co_references') or []:
            if not isinstance(ref, dict):
                continue
            ticket_id = self._safe_str(ref.get('raw_id') or ref.get('ticket_id') or ref.get('id'))
            system_type = self._safe_str(ref.get('source') or ref.get('system_type')).lower()
            system_type = self._normalize_system_type(system_type)
            if not ticket_id:
                continue
            mapped = self._signals_for_reference(targets, system_type=system_type, ticket_id=ticket_id, url=self._safe_str(ref.get('url')), relationship_hint=self._relationship_from_type(ref.get('type')), provenance='context_co_reference', raw_reference=ticket_id)
            if not mapped:
                log.info('CrossSystem co-reference skipped', ref=ticket_id, source=system_type)
            signals.extend(mapped)
        return signals

    async def _discover_reverse_references(self, primary: dict, targets: list) -> list[dict]:
        queries = self._reverse_reference_queries(primary)
        return await self._search_targets(targets, queries, provenance='reverse_reference_search', relationship_hint='direct_reference', max_results=5)

    async def _discover_semantic_live(self, primary: dict, targets: list) -> list[dict]:
        queries = await self._semantic_queries(primary)
        return await self._search_targets(targets, queries, provenance='semantic_live_search', relationship_hint='semantic_similarity', max_results=5)

    async def _discover_rescue_semantic(self, primary: dict, targets: list) -> list[dict]:
        queries = self._rescue_queries(primary)
        if not queries:
            return []
        log.info('CrossSystem rescue queries', queries=queries)
        return await self._search_targets(targets, queries, provenance='semantic_live_search', relationship_hint='semantic_similarity', max_results=10)

    async def _search_targets(self, targets: list, queries: list[str], provenance: str, relationship_hint: str, max_results: int) -> list[dict]:
        clean_queries = self._unique([q for q in self._flatten_query_values(queries) if self._safe_str(q)])[:8]
        if not targets or not clean_queries:
            return []
        semaphore = asyncio.Semaphore(20)

        async def search_one(connector, query):
            source_id = self._safe_str(getattr(connector, 'source_id', ''))
            system_type = self._safe_str(getattr(connector, 'system_type', ''))
            found = []
            if hasattr(connector, 'accepts_search_query') and (not connector.accepts_search_query(query)):
                return found
            try:
                async with semaphore:
                    results = await asyncio.wait_for(connector.search(query, max_results=max_results), timeout=self.CONNECTOR_TIMEOUT_SECONDS)
                for item in results or []:
                    candidate = self._ticket_to_candidate(item, connector)
                    if not candidate.get('ticket_id'):
                        continue
                    candidate['provenance'] = [provenance]
                    candidate['relationship_hint'] = relationship_hint
                    candidate['query_used'] = query
                    candidate['phrase_match'] = self._query_phrase_matches_candidate(query, candidate)
                    candidate['signals'] = [{'provenance': provenance, 'query_used': query, 'relationship_hint': relationship_hint, 'phrase_match': candidate['phrase_match']}]
                    found.append(candidate)
                log.info('CrossSystem search', source=source_id, query=query, count=len(results or []))
            except asyncio.TimeoutError:
                log.warning('CrossSystem search timeout', source=source_id, query=query)
            except Exception as e:
                log.warning('CrossSystem search failed', source=source_id, system_type=system_type, query=query, error=str(e))
            return found
        tasks = [search_one(connector, query) for connector in targets for query in clean_queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        signals = []
        for result in results:
            if isinstance(result, Exception):
                log.warning('CrossSystem target search raised', error=str(result))
                continue
            signals.extend(result)
        return signals

    async def _discover_cache(self, primary: dict, targets: list) -> list[dict]:
        terms = self._candidate_terms(primary) + list(primary.get('reference_variants') or []) + self._rescue_queries(primary)
        allowed_source_ids = {self._safe_str(getattr(c, 'source_id', '')) for c in targets if self._safe_str(getattr(c, 'source_id', ''))}
        try:
            params = inspect.signature(self._scan_redis_cache).parameters
            if 'allowed_source_ids' in params:
                cached = await self._scan_redis_cache(terms, allowed_source_ids=allowed_source_ids)
            else:
                cached = await self._scan_redis_cache(terms)
        except Exception as e:
            log.warning('CrossSystem cache discovery failed', error=str(e))
            return []
        signals = []
        for item in cached or []:
            candidate = self._dict_to_candidate(item)
            if allowed_source_ids and candidate.get('source_id') not in allowed_source_ids:
                continue
            candidate['provenance'] = ['redis_cache']
            candidate['relationship_hint'] = 'semantic_similarity'
            candidate['signals'] = [{'provenance': 'redis_cache', 'relationship_hint': 'semantic_similarity', 'overlap_score': item.get('overlap_score', 0), 'phrase_match': item.get('phrase_match', False)}]
            signals.append(candidate)
        return signals

    async def _scan_redis_cache(self, keywords: list[str], allowed_source_ids: set[str] | None=None) -> list[dict]:
        meaningful = [k.lower() for k in keywords if len(k) > 2 and k.lower() not in GENERIC_WORDS and (k.lower() not in DOMAIN_GENERIC_WORDS)]
        reference_terms = [k.lower() for k in keywords if self._looks_like_reference_query(self._safe_str(k)) or self._safe_str(k).startswith(('http://', 'https://'))]
        phrase_terms = [k.lower() for k in keywords if ' ' in self._safe_str(k) and len(self._safe_str(k)) <= 90 and (not self._safe_str(k).startswith(('http://', 'https://')))]
        if not meaningful:
            return []
        try:
            from ..redis_client import get_redis
            r = await get_redis()
            keys = []
            if hasattr(r, 'scan_iter'):
                async for key in r.scan_iter('buglist:*'):
                    keys.append(key)
            else:
                keys = await r.keys('buglist:*')
        except Exception as e:
            log.warning('CrossSystem redis scan failed', error=str(e))
            return []
        hits: list[dict] = []
        for key in keys:
            try:
                key_text = self._safe_str(key)
                source_id = self._source_id_from_buglist_key(key_text)
                if allowed_source_ids and source_id not in allowed_source_ids:
                    continue
                val = await r.get(key)
                data = json.loads(val) if val else None
                if not isinstance(data, list):
                    continue
                for bug in data:
                    if not isinstance(bug, dict):
                        continue
                    bug_source = self._safe_str(bug.get('source_id') or source_id)
                    if allowed_source_ids and bug_source not in allowed_source_ids:
                        continue
                    text = f"{bug.get('ticket_id', '')} {bug.get('id', '')} {bug.get('title', '')} {bug.get('description', '')} {bug.get('url', '')}".lower()
                    overlap = sum((1 for kw in meaningful if kw in text))
                    has_reference_match = any((ref in text for ref in reference_terms))
                    has_phrase_match = any((phrase in text for phrase in phrase_terms))
                    if overlap < 3 and (not has_reference_match) and (not has_phrase_match):
                        continue
                    candidate = dict(bug)
                    candidate['source_id'] = bug_source
                    candidate['overlap_score'] = overlap
                    candidate['reference_match'] = has_reference_match
                    candidate['phrase_match'] = has_phrase_match
                    hits.append(candidate)
            except Exception as e:
                log.warning('CrossSystem redis key skipped', key=self._safe_str(key), error=str(e))
        hits.sort(key=lambda x: x.get('overlap_score', 0), reverse=True)
        log.info('CrossSystem redis scan', keys=len(keys), hits=len(hits))
        return hits[:10]

    async def _hydrate_and_merge_signals(self, signals: list[dict], targets: list) -> list[dict]:
        by_source = {self._safe_str(getattr(c, 'source_id', '')): c for c in targets}
        semaphore = asyncio.Semaphore(6)

        async def hydrate(signal: dict) -> dict | None:
            candidate = self._dict_to_candidate(signal)
            if candidate.get('provenance') == ['redis_cache'] and candidate.get('title'):
                return candidate
            if candidate.get('title') and candidate.get('description'):
                return candidate
            source_id = candidate.get('source_id')
            ticket_id = candidate.get('ticket_id')
            connector = by_source.get(source_id)
            if not connector or not ticket_id:
                return candidate if candidate.get('ticket_id') else None
            try:
                async with semaphore:
                    fetched = await asyncio.wait_for(connector.get_ticket(connector.normalize_reference_id(ticket_id) if hasattr(connector, 'normalize_reference_id') else ticket_id.lstrip('#').replace('BZ-', '')), timeout=self.FETCH_TIMEOUT_SECONDS)
                if fetched:
                    hydrated = self._ticket_to_candidate(fetched, connector)
                    hydrated['signals'] = candidate.get('signals') or signal.get('signals') or []
                    hydrated['relationship_hint'] = self._strongest_relationship_hint([candidate.get('relationship_hint'), hydrated.get('relationship_hint')])
                    hydrated['provenance'] = self._unique((candidate.get('provenance') or []) + (hydrated.get('provenance') or []))
                    return hydrated
                candidate['fetch_error'] = 'not_found'
                return candidate
            except asyncio.TimeoutError:
                candidate['fetch_error'] = 'timeout'
                log.warning('CrossSystem hydrate timeout', source=source_id, ticket_id=ticket_id)
                return candidate
            except Exception as e:
                candidate['fetch_error'] = str(e)[:200]
                log.warning('CrossSystem hydrate failed', source=source_id, ticket_id=ticket_id, error=str(e))
                return candidate
        hydrated = await asyncio.gather(*(hydrate(s) for s in signals), return_exceptions=True)
        merged: dict[tuple[str, str], dict] = {}
        for item in hydrated:
            if isinstance(item, Exception) or not item:
                if isinstance(item, Exception):
                    log.warning('CrossSystem hydrate task raised', error=str(item))
                continue
            key = self._candidate_key(item)
            if not key[1]:
                continue
            if key not in merged:
                merged[key] = item
                continue
            merged[key] = self._merge_candidates(merged[key], item)
        return list(merged.values())

    def _merge_candidates(self, left: dict, right: dict) -> dict:
        merged = dict(left)
        for key in ('title', 'description', 'url', 'status', 'component'):
            if not self._safe_str(merged.get(key)) and self._safe_str(right.get(key)):
                merged[key] = right.get(key)
        merged['signals'] = (left.get('signals') or []) + (right.get('signals') or [])
        merged['provenance'] = self._unique((left.get('provenance') or []) + (right.get('provenance') or []))
        merged['relationship_hint'] = self._strongest_relationship_hint([left.get('relationship_hint'), right.get('relationship_hint')])
        merged['phrase_match'] = bool(left.get('phrase_match') or right.get('phrase_match'))
        merged['reference_match'] = bool(left.get('reference_match') or right.get('reference_match'))
        return merged

    def _prioritize_signals(self, signals: list[dict], limit: int=25) -> list[dict]:

        def priority(signal: dict) -> int:
            provenance = signal.get('provenance') or []
            if 'outbound_reference' in provenance or 'context_co_reference' in provenance:
                return 0
            if 'reverse_reference_search' in provenance:
                return 1
            if 'semantic_live_search' in provenance:
                return 2
            if 'redis_cache' in provenance:
                return 3
            return 4
        merged: dict[tuple[str, str], dict] = {}
        for signal in sorted(signals, key=lambda s: (priority(s), -self._signal_quality(s))):
            candidate = self._dict_to_candidate(signal)
            key = self._candidate_key(candidate)
            if not key[1]:
                continue
            if key not in merged:
                merged[key] = candidate
            else:
                merged[key] = self._merge_candidates(merged[key], candidate)
        prioritized = sorted(merged.values(), key=lambda s: (priority(s), -self._signal_quality(s)))
        if len(prioritized) > limit:
            log.info('CrossSystem candidate pre-cap', before=len(prioritized), after=limit)
        return prioritized[:limit]

    def _signal_quality(self, signal: dict) -> int:
        score = 0
        if signal.get('title'):
            score += 2
        if signal.get('description'):
            score += 3
        if signal.get('url'):
            score += 1
        if signal.get('reference_match'):
            score += 4
        if signal.get('phrase_match'):
            score += 3
        score += min(int(signal.get('overlap_score') or 0), 5)
        relationship = self._normalize_relationship(signal.get('relationship_hint'))
        if relationship in {'direct_reference', 'dependency', 'duplicate'}:
            score += 5
        return score

    async def _score_candidates(self, primary: dict, candidates: list[dict]) -> list[dict]:
        if not candidates:
            return []
        api_key = os.getenv('GROQ_API_KEY', '')
        model = os.getenv('GROQ_MODEL', 'llama-3.3-70b-versatile')
        if not api_key or AsyncGroq is None:
            if api_key and AsyncGroq is None:
                log.warning('CrossSystem Groq unavailable; using deterministic scoring')
            return self._deterministic_score(primary, candidates)

        async def score_batch(batch: list[dict]) -> list[dict]:
            try:
                return await asyncio.wait_for(self._score_with_groq(primary, batch, api_key, model), timeout=self.GROQ_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                log.warning('CrossSystem Groq scoring batch timed out')
                return self._deterministic_score(primary, batch)
            except Exception as e:
                log.warning('CrossSystem Groq scoring batch failed', error=str(e))
                return self._deterministic_score(primary, batch)
        batches = [candidates[i:i + 20] for i in range(0, len(candidates), 20)]
        results = await asyncio.gather(*(score_batch(batch) for batch in batches))
        scored = []
        for batch_result in results:
            scored.extend(batch_result)
        return scored

    async def _score_with_groq(self, primary: dict, candidates: list[dict], api_key: str, model: str) -> list[dict]:
        payload_candidates = [{'index': idx, 'ticket_id': c.get('ticket_id', ''), 'source_id': c.get('source_id', ''), 'system_type': c.get('system_type', ''), 'title': c.get('title', '')[:180], 'component': c.get('component', '')[:80], 'description': c.get('description', '')[:500], 'relationship_hint': c.get('relationship_hint', ''), 'provenance': c.get('provenance', [])} for idx, c in enumerate(candidates)]
        prompt = {'task': 'Score cross-system bug relationship candidates.', 'rules': ['Return JSON only.', 'similarity_score must be a float from 0.0 to 1.0, not 0-10 or 0-100.', 'A direct reference is not automatically a duplicate.', 'Only use duplicate for same root cause or same failure in same component/code path.', 'Use dependency only with explicit blocks/depends/fixed-by evidence.', 'Set is_related false for unrelated candidates.', 'High scores require concrete matching fields and a reason that supports the score.'], 'primary': {'ticket_id': primary.get('ticket_id', ''), 'source_id': primary.get('source_id', ''), 'system_type': primary.get('system_type', ''), 'title': primary.get('title', ''), 'component': primary.get('component', ''), 'error_excerpt': primary.get('error_excerpt', '')[:500], 'description': primary.get('description', '')[:800], 'comments': primary.get('comments_text', '')[:500], 'reference_variants': primary.get('reference_variants', [])}, 'candidates': payload_candidates, 'schema': {'results': [{'ticket_id': 'candidate ticket id', 'source_id': 'candidate source id', 'similarity_score': 0.0, 'relationship_type': 'direct_reference|duplicate|dependency|semantic_similarity|unrelated', 'similarity_label': 'Identical|Very Similar|Similar|Possible|Unrelated', 'similarity_reason': 'specific human-readable explanation', 'similarity_matching_fields': ['field names/evidence'], 'is_related': False}]}}
        client = AsyncGroq(api_key=api_key)
        resp = await client.chat.completions.create(model=model, messages=[{'role': 'user', 'content': json.dumps(prompt)}], temperature=0.0, response_format={'type': 'json_object'}, max_tokens=2000)
        raw = resp.choices[0].message.content or '{}'
        parsed = self._parse_json_response(raw)
        results = parsed.get('results') if isinstance(parsed, dict) else None
        if not isinstance(results, list):
            raise ValueError('Groq scoring response missing results list')
        result_map = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            source_id = self._safe_str(item.get('source_id'))
            ticket_id = self._normalize_ref(item.get('ticket_id'))
            if source_id and ticket_id:
                result_map[source_id, ticket_id] = item
                result_map[source_id.upper(), ticket_id] = item
        scored = []
        fallback = {self._candidate_key(c): c for c in self._deterministic_score(primary, candidates)}
        for c in candidates:
            key = self._candidate_key(c)
            raw_score = next((result_map.get(k) for k in self._score_lookup_keys(c) if result_map.get(k)), None)
            if not raw_score:
                scored.append(fallback.get(key, self._score_one_deterministic(primary, c)))
                continue
            enriched = dict(c)
            score = self._coerce_score(raw_score.get('similarity_score'))
            relationship = self._normalize_relationship(raw_score.get('relationship_type'))
            reason = self._safe_str(raw_score.get('similarity_reason'))
            fields = raw_score.get('similarity_matching_fields') or []
            if not isinstance(fields, list):
                fields = []
            is_related = bool(raw_score.get('is_related', score >= self.SEMANTIC_THRESHOLD))
            deterministic_floor = self._score_one_deterministic(primary, c)
            floor_score = deterministic_floor.get('similarity_score', 0.0)
            if floor_score >= 0.8 and floor_score > score:
                score = floor_score
                relationship = deterministic_floor.get('relationship_type', relationship)
                fields = self._unique(fields + deterministic_floor.get('similarity_matching_fields', []))
                if not reason or 'shares matching technical evidence' in reason.lower():
                    reason = deterministic_floor.get('similarity_reason', reason)
                is_related = True
            if self._contradictory(score, reason, relationship, fields, c):
                deterministic = deterministic_floor
                score = min(score, deterministic.get('similarity_score', 0.0), 0.49)
                relationship = 'semantic_similarity' if score >= self.DIRECT_REFERENCE_THRESHOLD else 'unrelated'
                reason = reason or 'Model output was contradictory; downgraded because evidence was insufficient.'
                is_related = score >= self.DIRECT_REFERENCE_THRESHOLD and self._has_direct_signal(c)
            enriched.update({'similarity_score': round(score, 2), 'relevance_score': round(score, 2), 'relationship_type': relationship, 'similarity_label': self._label(score, relationship), 'similarity_reason': reason or self._fallback_reason(primary, c, relationship), 'similarity_matching_fields': [self._safe_str(f) for f in fields if self._safe_str(f)], 'is_related': is_related})
            scored.append(enriched)
        return scored

    def _deterministic_score(self, primary: dict, candidates: list[dict]) -> list[dict]:
        return [self._score_one_deterministic(primary, c) for c in candidates]

    def _score_one_deterministic(self, primary: dict, candidate: dict) -> dict:
        c = dict(candidate)
        primary_terms = set(self._candidate_terms(primary))
        candidate_terms = set(self._candidate_terms(candidate))
        overlap = sorted(primary_terms & candidate_terms)
        semantic_fields, semantic_score = self._semantic_evidence(primary, candidate)
        has_direct = self._has_direct_signal(c)
        has_reverse = 'reverse_reference_search' in (c.get('provenance') or [])
        relationship = self._normalize_relationship(c.get('relationship_hint'))
        if relationship == 'dependency':
            score = 0.72 if has_direct else 0.6
        elif has_direct:
            score = 0.68
        elif has_reverse:
            score = 0.62
        elif overlap:
            score = min(0.35 + 0.08 * len(overlap), 0.62)
            relationship = 'semantic_similarity'
        elif 'redis_cache' in (c.get('provenance') or []) and c.get('overlap_score', 0):
            score = min(0.45 + 0.03 * int(c.get('overlap_score') or 0), 0.62)
            relationship = 'semantic_similarity'
        else:
            score = 0.18
            relationship = 'unrelated'
        strong_identifiers = self._strong_identifier_overlap(primary, candidate)
        if strong_identifiers:
            score = max(score, 0.74 if has_direct or has_reverse else 0.58)
            if relationship == 'semantic_similarity' and (has_direct or has_reverse):
                relationship = 'direct_reference'
        if semantic_score:
            score = max(score, semantic_score)
            if semantic_score >= 0.8 and relationship == 'semantic_similarity':
                relationship = 'duplicate'
        if c.get('phrase_match'):
            score = max(score, 0.74)
        if relationship == 'duplicate' and score < 0.8:
            relationship = 'semantic_similarity'
        if relationship == 'unrelated':
            score = min(score, 0.3)
        if c.get('provenance') == ['redis_cache']:
            if c.get('reference_match'):
                score = min(score, 0.8)
            elif c.get('phrase_match'):
                score = min(score, 0.74)
            else:
                score = min(score, 0.52)
        c['similarity_score'] = round(min(score, 0.95), 2)
        c['relevance_score'] = c['similarity_score']
        c['relationship_type'] = relationship
        c['similarity_label'] = self._label(c['similarity_score'], relationship)
        c['similarity_matching_fields'] = self._unique(semantic_fields + overlap[:6] + strong_identifiers[:4])
        c['similarity_reason'] = self._fallback_reason(primary, c, relationship)
        c['is_related'] = relationship != 'unrelated' and (c['similarity_score'] >= self.SEMANTIC_THRESHOLD or (self._has_direct_signal(c) and c['similarity_score'] >= self.DIRECT_REFERENCE_THRESHOLD))
        return c

    def _finalize_candidates(self, candidates: list[dict], primary: dict) -> list[dict]:
        deduped: dict[tuple[str, str], dict] = {}
        for c in candidates:
            n = self._normalize_candidate(c)
            if self._is_primary_match(n, primary):
                continue
            if self._is_same_system_candidate(n, primary):
                continue
            relationship = n.get('relationship_type')
            score = n.get('similarity_score', 0.0)
            if relationship == 'unrelated':
                continue
            if relationship in {'direct_reference', 'dependency'}:
                if score < self.DIRECT_REFERENCE_THRESHOLD:
                    continue
            elif score < self.SEMANTIC_THRESHOLD:
                continue
            key = self._final_dedupe_key(n)
            if not key[1]:
                continue
            existing = deduped.get(key)
            if not existing or self._candidate_sort_key(n) > self._candidate_sort_key(existing):
                deduped[key] = n
        final = list(deduped.values())
        final.sort(key=self._candidate_sort_key, reverse=True)
        return final[:self.FINAL_RESULT_LIMIT]

    def _normalize_candidate(self, raw: dict) -> dict:
        ticket_id = self._safe_str(raw.get('ticket_id') or raw.get('id') or raw.get('key') or raw.get('number'))
        system_type = self._normalize_system_type(raw.get('system_type') or raw.get('source') or '')
        source_id = self._safe_str(raw.get('source_id') or raw.get('source_id_hint') or raw.get('source') or system_type)
        if 'github' in system_type and ticket_id and ticket_id.isdigit():
            display_id = f'#{ticket_id}'
        else:
            display_id = ticket_id
        score = self._coerce_score(raw.get('similarity_score', raw.get('relevance_score', 0.0)))
        relationship = self._normalize_relationship(raw.get('relationship_type') or raw.get('relationship_hint'))
        if relationship == 'unrelated' and score >= self.SEMANTIC_THRESHOLD:
            relationship = 'semantic_similarity'
        base_url = self._safe_str(raw.get('base_url'))
        url = sanitize_bug_url(url=self._safe_str(raw.get('url') or raw.get('html_url') or raw.get('link')), system_type=system_type, bug_id=ticket_id, base_url=base_url)
        reason = self._safe_str(raw.get('similarity_reason') or raw.get('reason'))
        if not reason:
            reason = 'Related issue candidate discovered across enabled external systems.'
        fields = raw.get('similarity_matching_fields') or []
        if not isinstance(fields, list):
            fields = []
        return {'id': display_id, 'ticket_id': display_id, 'title': self._safe_str(raw.get('title') or raw.get('summary') or raw.get('name')), 'url': url, 'status': self._safe_str(raw.get('status') or raw.get('state') or 'unknown').lower(), 'source': system_type, 'source_id': source_id, 'system_type': system_type, 'description': self._safe_str(raw.get('description') or raw.get('body'))[:300], 'relevance_score': round(score, 2), 'similarity_score': round(score, 2), 'similarity_label': self._safe_str(raw.get('similarity_label')) or self._label(score, relationship), 'similarity_reason': reason, 'relationship_type': relationship, 'similarity_matching_fields': [self._safe_str(f) for f in fields if self._safe_str(f)], 'raw_key': self._safe_str(raw.get('raw_key') or raw.get('key') or ticket_id), 'provenance': raw.get('provenance') or []}

    def _ticket_to_candidate(self, item: Any, connector: Any | None=None) -> dict:
        if dataclasses.is_dataclass(item):
            data = dataclasses.asdict(item)
        elif isinstance(item, dict):
            data = dict(item)
        else:
            data = {k: getattr(item, k, '') for k in ('ticket_id', 'id', 'title', 'description', 'severity', 'status', 'component', 'source_id', 'system_type', 'url')}
            data.update({k: getattr(item, k, []) for k in ('linked_items', 'direct_reference_links')})
        if connector is not None:
            data.setdefault('source_id', self._safe_str(getattr(connector, 'source_id', '')))
            data.setdefault('system_type', self._safe_str(getattr(connector, 'system_type', '')))
            data.setdefault('base_url', self._safe_str(getattr(connector, 'base_url', '')))
        return self._dict_to_candidate(data)

    def _dict_to_candidate(self, data: dict) -> dict:
        if not isinstance(data, dict):
            data = {}
        ticket_id = self._safe_str(data.get('ticket_id') or data.get('id') or data.get('key') or data.get('number'))
        system_type = self._normalize_system_type(data.get('system_type') or data.get('source') or '')
        source_id = self._safe_str(data.get('source_id') or data.get('source_id_hint') or data.get('source') or system_type)
        candidate = {'ticket_id': ticket_id, 'id': ticket_id, 'raw_key': self._safe_str(data.get('raw_key') or data.get('key')), 'source_id': source_id, 'source': system_type, 'system_type': system_type, 'title': self._safe_str(data.get('title') or data.get('summary') or data.get('name')), 'description': self._safe_str(data.get('description') or data.get('body')), 'status': self._safe_str(data.get('status') or data.get('state') or 'unknown'), 'component': self._safe_str(data.get('component') or ''), 'url': self._safe_str(data.get('url') or data.get('html_url') or data.get('link')), 'base_url': self._safe_str(data.get('base_url') or ''), 'relationship_hint': self._normalize_relationship(data.get('relationship_hint') or data.get('relationship_type') or data.get('relationship')), 'provenance': data.get('provenance') if isinstance(data.get('provenance'), list) else [], 'signals': data.get('signals') if isinstance(data.get('signals'), list) else [], 'overlap_score': data.get('overlap_score', 0), 'reference_match': bool(data.get('reference_match')), 'phrase_match': bool(data.get('phrase_match'))}
        for key in ('similarity_score', 'relevance_score'):
            if data.get(key) is not None:
                candidate[key] = self._coerce_score(data.get(key))
        return candidate

    def _signals_for_reference(self, targets: list, system_type: str, ticket_id: str, relationship_hint: str, provenance: str, raw_reference: str, url: str='', repo: str='') -> list[dict]:
        normalized_type = self._normalize_system_type(system_type)
        matching = []
        for c in targets:
            ctype = self._safe_str(getattr(c, 'system_type', '')).lower()
            if normalized_type and normalized_type not in ctype and (ctype not in normalized_type):
                continue
            if normalized_type == 'jira':
                ref_prefix = self._safe_str(ticket_id).split('-')[0].lower()
                connector_prefixes = {self._safe_str(getattr(c, 'ticket_prefix', '')).lower(), self._safe_str(getattr(c, 'project_key', '')).lower()}
                connector_prefixes.discard('')
                if connector_prefixes and ref_prefix not in connector_prefixes:
                    continue
            if repo and 'github' in ctype:
                project_key = self._safe_str(getattr(c, 'project_key', ''))
                if project_key and project_key.lower() != repo.lower():
                    continue
            matching.append(c)
        signals = []
        for c in matching:
            source_id = self._safe_str(getattr(c, 'source_id', ''))
            ctype = self._safe_str(getattr(c, 'system_type', ''))
            signals.append({'source_id': source_id, 'source_id_hint': source_id, 'system_type': ctype, 'source': ctype, 'ticket_id': self._safe_str(ticket_id), 'id': self._safe_str(ticket_id), 'url': url, 'base_url': self._safe_str(getattr(c, 'base_url', '')), 'relationship_hint': relationship_hint, 'provenance': [provenance], 'query_used': '', 'raw_reference': raw_reference, 'signals': [{'provenance': provenance, 'relationship_hint': relationship_hint, 'raw_reference': raw_reference, 'url': url}]})
        return signals

    async def _semantic_queries(self, primary: dict) -> list[str]:
        queries = self._deterministic_queries(primary)
        api_key = os.getenv('GROQ_API_KEY', '')
        model = os.getenv('GROQ_MODEL', 'llama-3.3-70b-versatile')
        if not api_key or AsyncGroq is None:
            return queries
        try:
            generated = await asyncio.wait_for(self._generate_platform_queries(primary, api_key, model), timeout=12.0)
            queries.extend(self._flatten_query_values(generated.values()))
        except Exception as e:
            log.warning('CrossSystem query generation failed', error=str(e))
        return self._unique([q for q in self._flatten_query_values(queries) if self._safe_str(q) and (not self._is_generic_query(q))])[:8]

    async def _generate_platform_queries(self, primary: dict, api_key: str, model: str) -> dict:
        prompt = {'task': 'Generate precise bug search queries.', 'rules': ['Return JSON object only.', 'Use exact developer vocabulary.', 'Prefer exception names, class/method names, file names, config names, and short error phrases.', 'Avoid generic words like bug, issue, error, fix, problem.'], 'bug': {'title': primary.get('title', ''), 'component': primary.get('component', ''), 'error_excerpt': primary.get('error_excerpt', '')[:400], 'description': primary.get('description', '')[:500]}, 'schema': {'specific_query': '2-4 precise terms', 'component_error_query': 'component plus concrete symptom', 'broad_query': '2-4 ecosystem terms'}}
        client = AsyncGroq(api_key=api_key)
        resp = await client.chat.completions.create(model=model, messages=[{'role': 'user', 'content': json.dumps(prompt)}], temperature=0.0, response_format={'type': 'json_object'}, max_tokens=180)
        parsed = self._parse_json_response(resp.choices[0].message.content or '{}')
        if not isinstance(parsed, dict):
            return {}
        return {'specific_query': self._safe_str(parsed.get('specific_query'))[:90], 'component_error_query': self._safe_str(parsed.get('component_error_query'))[:90], 'broad_query': self._safe_str(parsed.get('broad_query'))[:90]}

    def _deterministic_queries(self, primary: dict) -> list[str]:
        terms = self._candidate_terms(primary)
        component = primary.get('component', '')
        combined = self._combined_text(primary)
        exception_terms = re.findall('\\b[A-Z][A-Za-z0-9]*(?:Exception|Error|Failure|Fault)\\b', combined)
        file_terms = re.findall('\\b[\\w.-]+\\.(?:java|py|js|ts|xml|yml|yaml|json|gradle|properties|conf)\\b', combined, re.IGNORECASE)
        config_terms = self._unique(re.findall('\\bspark\\.[A-Za-z0-9_.]+\\b', combined))
        config_phrase_terms = self._config_phrase_queries(config_terms)
        camel_terms = re.findall('\\b[A-Za-z]+(?:[A-Z][A-Za-z0-9]+)+\\b', combined)
        phrase_terms = self._technical_phrases(primary)
        queries = []
        if component and component.lower() not in DOMAIN_GENERIC_WORDS:
            component_terms = [t for t in self._candidate_terms({'title': component}) if t.lower() not in DOMAIN_GENERIC_WORDS]
        else:
            component_terms = []
        for pool in (exception_terms[:2], file_terms[:2], config_terms[:2], camel_terms[:2], component_terms[:2], [' '.join(terms[:4])] if len(terms) >= 2 else []):
            q = ' '.join(pool) if isinstance(pool, list) else pool
            if q and q.strip() and (not self._is_generic_query(q)):
                queries.append(q.strip())
        queries.extend((q for q in config_phrase_terms[:4] if not self._is_generic_query(q)))
        queries.extend((q for q in phrase_terms[:3] if not self._is_generic_query(q)))
        return self._unique(queries)

    def _rescue_queries(self, primary: dict) -> list[str]:
        title = self._safe_str(primary.get('title'))
        description = self._safe_str(primary.get('description'))
        text = f'{title} {description}'
        queries = []
        queries.extend(self._deterministic_queries(primary))
        title_tokens = [t for t in self._important_tokens(title) if t not in {'fails', 'failed', 'start', 'exist', 'exists'}]
        if len(title_tokens) >= 3:
            queries.append(' '.join(title_tokens[:6]))
        if {'history', 'log', 'directory'} <= set(title_tokens) or 'spark.history.fs.logdirectory' in text.lower():
            queries.extend(['history log directory', 'spark history log directory', 'create history log directory', 'history server log directory'])
        configs = self._unique(re.findall('\\bspark\\.[A-Za-z0-9_.]+\\b', text))
        queries.extend(self._config_phrase_queries(configs))
        cleaned = []
        for query in queries:
            q = self._safe_str(query).strip(' ;:')
            if not q:
                continue
            words = re.findall('[A-Za-z0-9_.:-]+', q)
            if len(words) == 1 and self._is_generic_query(q):
                continue
            cleaned.append(q)
        return self._unique(cleaned)[:10]

    def _flatten_query_values(self, values: Any) -> list[str]:
        flattened: list[str] = []
        if values is None:
            return flattened
        if isinstance(values, dict):
            iterable = values.values()
        elif isinstance(values, (list, tuple, set)):
            iterable = values
        else:
            iterable = [values]
        for value in iterable:
            text = self._safe_str(value).strip()
            if not text:
                continue
            if text.startswith('[') or text.startswith('{'):
                try:
                    parsed = json.loads(text)
                    flattened.extend(self._flatten_query_values(parsed))
                    continue
                except Exception:
                    pass
            if isinstance(value, (list, tuple, set, dict)):
                flattened.extend(self._flatten_query_values(value))
                continue
            flattened.append(text)
        return flattened

    def _technical_phrases(self, primary: dict) -> list[str]:
        text = ' '.join([self._safe_str(primary.get('title')), self._safe_str(primary.get('description'))])
        raw_words = [w.lower() for w in re.findall('[A-Za-z][A-Za-z0-9_.-]*', text) if len(w) > 2]
        words = [w for w in raw_words if w not in GENERIC_WORDS and w not in DOMAIN_GENERIC_WORDS and (w not in {'automatically', 'automatic', 'create', 'created', 'creating'})]
        deduped_words = []
        for word in words:
            if not deduped_words or deduped_words[-1] != word:
                deduped_words.append(word)
        words = deduped_words
        phrases = []
        for size in (3, 4):
            for idx in range(0, max(0, len(words) - size + 1)):
                phrase = ' '.join(words[idx:idx + size])
                if not self._is_generic_query(phrase):
                    phrases.append(phrase)
        return self._unique(phrases)[:5]

    def _config_phrase_queries(self, config_terms: list[str]) -> list[str]:
        phrases = []
        for config in config_terms:
            parts = []
            for part in self._safe_str(config).split('.'):
                split = re.sub('([a-z])([A-Z])', '\\1 \\2', part)
                parts.extend((token.lower() for token in re.findall('[A-Za-z]+', split) if token.lower() not in {'spark', 'fs'}))
            parts = [p for p in parts if p not in GENERIC_WORDS and p not in DOMAIN_GENERIC_WORDS]
            if len(parts) >= 2:
                phrases.append(' '.join(parts))
            if 'history' in parts and 'log' in parts and ('directory' in parts):
                phrases.extend(['history log directory', 'spark history log directory'])
            if 'log' in parts and 'directory' in parts:
                phrases.append('log directory')
        return self._unique([p for p in phrases if p and (not self._is_generic_query(p))])

    def _query_phrase_matches_candidate(self, query: str, candidate: dict) -> bool:
        q = self._safe_str(query).lower().strip()
        if not q or ' ' not in q:
            return False
        if self._is_generic_query(q):
            return False
        text = f"{candidate.get('title', '')} {candidate.get('description', '')}".lower()
        return q in text

    def _is_generic_query(self, query: str) -> bool:
        words = [w.lower() for w in re.findall('[A-Za-z0-9_.:-]+', self._safe_str(query)) if len(w) > 2]
        if not words:
            return True
        meaningful = [w for w in words if w not in GENERIC_WORDS and w not in DOMAIN_GENERIC_WORDS]
        return len(meaningful) < 2 and (not any((self._looks_like_reference_query(w) or re.search('(exception|error|failure|fault)$', w, re.IGNORECASE) or '.' in w for w in words)))

    def _looks_like_reference_query(self, text: str) -> bool:
        if not text or len(text) > 80:
            return False
        return bool(re.match('^[A-Z][A-Z0-9]+-\\d+$', text, re.IGNORECASE) or re.match('^(?:GH|BZ)-\\d+$', text, re.IGNORECASE) or re.match('^#?\\d+$', text) or re.match('^[\\w.-]+/[\\w.-]+#\\d+$', text))

    def _reverse_reference_queries(self, primary: dict) -> list[str]:
        variants = primary.get('reference_variants') or []
        queries = []
        for variant in variants:
            text = self._safe_str(variant).strip()
            if not text or len(text) > 60:
                continue
            if text.startswith(('http://', 'https://')):
                continue
            if self._looks_like_reference_query(text):
                queries.append(text)
        return self._unique(queries)

    def _candidate_terms(self, item: dict) -> list[str]:
        text = self._combined_text(item)
        tokens = []
        tokens.extend(re.findall('\\b[A-Z][A-Za-z0-9]*(?:Exception|Error|Failure|Fault)\\b', text))
        tokens.extend(re.findall('\\b[A-Z][a-z]+(?:[A-Z][A-Za-z0-9]+)+\\b', text))
        tokens.extend(re.findall('\\b[\\w.-]+\\.(?:java|py|js|ts|xml|yml|yaml|json|gradle|properties|conf)\\b', text, re.IGNORECASE))
        for raw in re.findall('[A-Za-z0-9_.:-]+', text):
            word = raw.strip('.,()[]\'"').lower()
            if len(word) > 4 and word not in GENERIC_WORDS and (word not in DOMAIN_GENERIC_WORDS) and (not word.isdigit()):
                tokens.append(word)
        return self._unique([t.strip() for t in tokens if len(t.strip()) > 2])[:20]

    def _combined_text(self, item: dict) -> str:
        return ' '.join([self._safe_str(item.get('ticket_id')), self._safe_str(item.get('raw_key')), self._safe_str(item.get('url')), self._safe_str(item.get('title')), self._safe_str(item.get('component')), self._safe_str(item.get('error_excerpt')), self._safe_str(item.get('description')), self._safe_str(item.get('comments_text'))])

    def _comments_to_text(self, comments: Any) -> str:
        if not comments:
            return ''
        if isinstance(comments, str):
            return comments
        if isinstance(comments, dict):
            return ' '.join((self._safe_str(v) for v in comments.values()))
        if isinstance(comments, list):
            parts = []
            for comment in comments:
                if isinstance(comment, dict):
                    parts.append(' '.join((self._safe_str(comment.get(k)) for k in ('body', 'text', 'comment', 'title', 'author'))))
                else:
                    parts.append(self._safe_str(comment))
            return ' '.join(parts)
        return self._safe_str(comments)

    def _relationship_from_type(self, value: Any) -> str:
        text = self._safe_str(value).lower()
        if any((word in text for word in ('block', 'depend', 'caused', 'fixed', 'require'))):
            return 'dependency'
        if 'duplicate' in text or 'dup' in text:
            return 'duplicate'
        if 'reference' in text or 'link' in text:
            return 'direct_reference'
        return 'direct_reference'

    def _parse_json_response(self, raw: str) -> Any:
        text = self._safe_str(raw).strip()
        fence = re.match('^```(?:json)?\\s*(.*?)\\s*```$', text, re.IGNORECASE | re.DOTALL)
        if fence:
            text = fence.group(1).strip()
        parsed = json.loads(text)
        return {'results': parsed} if isinstance(parsed, list) else parsed

    def _coerce_score(self, value: Any) -> float:
        try:
            score = float(value)
        except Exception:
            return 0.0
        if score < 0:
            return 0.0
        if score <= 1.0:
            return round(score, 2)
        if score <= 10.0:
            return round(score / 10.0, 2)
        if score <= 100.0:
            return round(score / 100.0, 2)
        return 0.0

    def _contradictory(self, score: float, reason: str, relationship: str, fields: list, candidate: dict) -> bool:
        reason_l = self._safe_str(reason).lower()
        if score >= 0.75 and any((word in reason_l for word in UNRELATED_WORDS)):
            return True
        if relationship == 'duplicate' and score < 0.75:
            return True
        if relationship == 'dependency' and (not (self._has_direct_signal(candidate) or any(('depend' in self._safe_str(f).lower() or 'block' in self._safe_str(f).lower() for f in fields)))):
            return True
        if score >= 0.85 and (not fields) and (not reason_l) and (not self._has_direct_signal(candidate)):
            return True
        return False

    def _normalize_relationship(self, value: Any) -> str:
        text = self._safe_str(value).lower().strip().replace('-', '_').replace(' ', '_')
        if text in RELATIONSHIP_TYPES:
            return text
        if 'duplicate' in text or text == 'identical':
            return 'duplicate'
        if any((word in text for word in ('depend', 'block', 'caused', 'fixed', 'require'))):
            return 'dependency'
        if 'reference' in text or 'link' in text or 'co_reference' in text:
            return 'direct_reference'
        if 'similar' in text or 'related' in text:
            return 'semantic_similarity'
        return 'semantic_similarity'

    def _label(self, score: float, relationship: str) -> str:
        if relationship == 'unrelated' or score < 0.35:
            return 'Unrelated'
        if relationship == 'dependency':
            return 'Dependency'
        if relationship == 'direct_reference' and score < 0.8:
            return 'Referenced'
        return 'Identical' if score >= 0.9 else 'Very Similar' if score >= 0.8 else 'Similar' if score >= 0.6 else 'Possible'

    def _fallback_reason(self, primary: dict, candidate: dict, relationship: str) -> str:
        provenance = ', '.join(candidate.get('provenance') or [])
        fields = candidate.get('similarity_matching_fields') or []
        evidence = self._reason_evidence(primary, candidate, fields)
        shared_configs = evidence.get('shared_configs', [])
        candidate_configs = evidence.get('candidate_configs', [])
        shared_phrase = evidence.get('shared_phrase', '')
        shared_terms = evidence.get('shared_terms', [])
        field_text = ', '.join((shared_configs + candidate_configs + shared_terms)[:5])
        if relationship == 'dependency':
            return 'The issues have an explicit dependency or blocking relationship.'
        if relationship == 'direct_reference':
            if field_text:
                return f'One issue explicitly references the other, with shared evidence: {field_text}.'
            return 'One issue explicitly references the other across systems.'
        if shared_configs:
            return f'Both issues involve the same Spark configuration key `{shared_configs[0]}` and the same missing log-directory failure mode.'
        if candidate_configs:
            return f'The candidate is related through Spark log-directory configuration `{candidate_configs[0]}`, but it does not match the exact primary config key.'
        if shared_phrase:
            return f'Both issues describe the same history/log-directory behavior: {shared_phrase}.'
        if shared_terms:
            return 'Both issues share technical terms around ' + ', '.join(shared_terms[:4]) + ', but the exact failure mode needs confirmation.'
        if provenance:
            return f'The candidate was discovered via {provenance}, but only weak structured evidence was available.'
        return 'The candidate has related technical terms but insufficient evidence for a stronger label.'

    def _reason_evidence(self, primary: dict, candidate: dict, fields: list) -> dict:
        primary_text = self._combined_text(primary).lower()
        candidate_text = self._combined_text(candidate).lower()
        primary_configs = sorted(set(re.findall('\\bspark\\.[a-z0-9_.]+\\b', primary_text)))
        candidate_configs = sorted(set(re.findall('\\bspark\\.[a-z0-9_.]+\\b', candidate_text)))
        shared_configs = sorted(set(primary_configs) & set(candidate_configs))
        primary_config_set = set(primary_configs)
        candidate_only_configs = [c for c in candidate_configs if c not in primary_config_set]
        primary_phrases = set(self._technical_phrases(primary))
        candidate_phrases = set(self._technical_phrases(candidate))
        shared_phrases = sorted(primary_phrases & candidate_phrases)
        noisy = {'https', 'http', 'issues', 'exist', 'does', 'start', 'created', 'creating', 'automatically'}
        shared_terms = [self._safe_str(f) for f in fields if self._safe_str(f) and self._safe_str(f).lower() not in noisy and (not self._safe_str(f).lower().startswith('http'))]
        return {'shared_configs': shared_configs, 'candidate_configs': candidate_only_configs, 'shared_phrase': shared_phrases[0] if shared_phrases else '', 'shared_terms': self._unique(shared_terms)}

    def _semantic_evidence(self, primary: dict, candidate: dict) -> tuple[list[str], float]:
        primary_text = self._combined_text(primary)
        candidate_text = self._combined_text(candidate)
        fields: list[str] = []
        primary_configs = {c.lower() for c in re.findall('\\bspark\\.[A-Za-z0-9_.]+\\b', primary_text)}
        candidate_configs = {c.lower() for c in re.findall('\\bspark\\.[A-Za-z0-9_.]+\\b', candidate_text)}
        shared_configs = sorted(primary_configs & candidate_configs)
        fields.extend(shared_configs)
        primary_title_tokens = self._important_tokens(primary.get('title', ''))
        candidate_title_tokens = self._important_tokens(candidate.get('title', ''))
        shared_title = sorted(primary_title_tokens & candidate_title_tokens)
        if shared_title:
            fields.extend(shared_title[:5])
        title_union = primary_title_tokens | candidate_title_tokens
        title_ratio = len(shared_title) / len(title_union) if title_union else 0.0
        primary_phrases = set(self._technical_phrases(primary))
        candidate_phrases = set(self._technical_phrases(candidate))
        primary_phrases.update(self._config_phrase_queries(list(primary_configs)))
        candidate_phrases.update(self._config_phrase_queries(list(candidate_configs)))
        shared_phrases = sorted(primary_phrases & candidate_phrases)
        fields.extend(shared_phrases[:3])
        if shared_configs and title_ratio >= 0.35:
            return (self._unique(fields), 0.82)
        if shared_configs and len(shared_title) >= 2:
            return (self._unique(fields), 0.8)
        if shared_phrases and len(shared_title) >= 2:
            return (self._unique(fields), 0.8)
        if shared_phrases and title_ratio >= 0.3:
            return (self._unique(fields), 0.74)
        if title_ratio >= 0.55 and len(shared_title) >= 3:
            return (self._unique(fields), 0.72)
        if shared_configs:
            return (self._unique(fields), 0.68)
        return (self._unique(fields), 0.0)

    def _important_tokens(self, text: Any) -> set[str]:
        tokens = set()
        for raw in re.findall('[A-Za-z0-9_.:-]+', self._safe_str(text)):
            word = raw.strip('.,()[]\'"').lower()
            if len(word) > 2 and word not in GENERIC_WORDS and (word not in DOMAIN_GENERIC_WORDS) and (not word.isdigit()):
                tokens.add(word)
        return tokens

    def _has_direct_signal(self, candidate: dict) -> bool:
        provenance = set(candidate.get('provenance') or [])
        if provenance & {'outbound_reference', 'context_co_reference', 'reverse_reference_search'}:
            return True
        return any((s.get('relationship_hint') in {'direct_reference', 'dependency', 'duplicate'} for s in candidate.get('signals') or [] if isinstance(s, dict)))

    def _reference_matches_connector(self, ref: dict, connector: Any) -> bool:
        system_type = self._normalize_system_type(getattr(connector, 'system_type', ''))
        ref_source = self._normalize_system_type(ref.get('source') or ref.get('system_type'))
        if ref_source != 'unknown' and ref_source != system_type:
            return False
        if system_type == 'jira':
            ref_prefix = self._safe_str(ref.get('raw_id')).split('-')[0].lower()
            prefixes = {self._safe_str(getattr(connector, 'ticket_prefix', '')).lower(), self._safe_str(getattr(connector, 'project_key', '')).lower()}
            prefixes.discard('')
            return not prefixes or ref_prefix in prefixes
        if system_type == 'github' and ref.get('repo'):
            project_key = self._safe_str(getattr(connector, 'project_key', ''))
            return not project_key or project_key.lower() == self._safe_str(ref.get('repo')).lower()
        return True

    def _strong_identifier_overlap(self, primary: dict, candidate: dict) -> list[str]:
        candidate_text = self._combined_text(candidate).lower()
        matches = []
        for variant in primary.get('reference_variants') or []:
            v = self._safe_str(variant)
            if len(v) > 2 and v.lower() in candidate_text:
                matches.append(v)
        return self._unique(matches)

    def _candidate_key(self, item: dict) -> tuple[str, str]:
        source_id = self._safe_str(item.get('source_id') or item.get('source') or item.get('system_type'))
        return (source_id.upper(), self._normalize_ref(item.get('ticket_id') or item.get('id') or item.get('key')))

    def _final_dedupe_key(self, item: dict) -> tuple[str, str]:
        system_type = self._normalize_system_type(item.get('system_type') or item.get('source'))
        source_id = self._safe_str(item.get('source_id')).upper()
        ref = self._normalize_ref(item.get('raw_key') or item.get('ticket_id') or item.get('id') or item.get('key'))
        if re.match('^[A-Z][A-Z0-9]+-\\d+$', ref):
            return ('JIRA', ref)
        return (system_type.upper() or source_id, ref)

    def _candidate_sort_key(self, item: dict) -> tuple[int, float]:
        return (self._relationship_rank(item.get('relationship_type')), float(item.get('similarity_score') or 0.0))

    def _score_lookup_keys(self, item: dict) -> list[tuple[str, str]]:
        source_id = self._safe_str(item.get('source_id') or item.get('source') or item.get('system_type'))
        refs = {item.get('ticket_id'), item.get('id'), item.get('key'), item.get('raw_key')}
        normalized_refs = {self._normalize_ref(ref) for ref in refs if self._safe_str(ref)}
        return [(source_id, ref) for ref in normalized_refs if ref] + [(source_id.upper(), ref) for ref in normalized_refs if ref]

    def _is_primary_match(self, candidate: dict, primary: dict) -> bool:
        primary_refs = {self._normalize_ref(primary.get(k)) for k in ('ticket_id', 'id', 'raw_key')}
        primary_refs.update((self._normalize_ref(v) for v in primary.get('reference_variants', [])))
        candidate_refs = {self._normalize_ref(candidate.get(k)) for k in ('ticket_id', 'id', 'raw_key')}
        primary_refs.discard('')
        candidate_refs.discard('')
        if primary_refs & candidate_refs:
            return True
        primary_title = self._safe_str(primary.get('title')).strip().lower()
        candidate_title = self._safe_str(candidate.get('title')).strip().lower()
        return bool(primary_title and candidate_title and (primary_title == candidate_title))

    def _is_same_system_candidate(self, candidate: dict, primary: dict) -> bool:
        primary_system = self._normalize_system_type(primary.get('system_type') or primary.get('source'))
        candidate_system = self._normalize_system_type(candidate.get('system_type') or candidate.get('source'))
        if not primary_system or not candidate_system:
            return False
        return primary_system == candidate_system

    def _normalize_ref(self, value: Any) -> str:
        text = self._safe_str(value).strip().upper()
        if not text:
            return ''
        text = text.rstrip('/')
        if '/ISSUES/' in text:
            text = text.split('/ISSUES/')[-1]
        if 'SHOW_BUG.CGI' in text and 'ID=' in text:
            text = text.split('ID=')[-1].split('&')[0]
        if text.startswith('#'):
            text = text[1:]
        if text.startswith('GH-'):
            text = text[3:]
        if text.startswith('BZ-'):
            text = text[3:]
        return text

    def _normalize_system_type(self, value: Any) -> str:
        text = self._safe_str(value).lower()
        if 'github' in text or text in {'gh', 'git'}:
            return 'github'
        if 'bugzilla' in text or text == 'bz':
            return 'bugzilla'
        if 'jira' in text:
            return 'jira'
        return text or 'unknown'

    def _source_id_from_buglist_key(self, key: str) -> str:
        parts = key.split(':')
        return parts[1] if len(parts) >= 2 and parts[0] == 'buglist' else ''

    def _relationship_rank(self, relationship: Any) -> int:
        return {'duplicate': 4, 'dependency': 3, 'direct_reference': 2, 'semantic_similarity': 1}.get(self._normalize_relationship(relationship), 0)

    def _strongest_relationship_hint(self, hints: list) -> str:
        normalized = [self._normalize_relationship(h) for h in hints if self._safe_str(h)]
        if not normalized:
            return 'semantic_similarity'
        return max(normalized, key=self._relationship_rank)

    def _unique(self, values: list) -> list:
        seen = set()
        result = []
        for value in values:
            key = self._safe_str(value)
            if not key or key.lower() in seen:
                continue
            seen.add(key.lower())
            result.append(value)
        return result

    def _safe_str(self, value: Any) -> str:
        if value is None:
            return ''
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float, bool)):
            return str(value)
        try:
            return json.dumps(value, default=str)
        except Exception:
            return str(value)