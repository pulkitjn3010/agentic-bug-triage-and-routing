import asyncio
import base64
import json
import os
import re
import math
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv()

# ─── Workspace Configuration ───────────────────────────────────────
# Supports JSON array in CONFLUENCE_WORKSPACES env var
# OR falls back to single-instance env vars
# OR uses public wikis (no auth required)


def load_workspaces() -> list[dict]:
    raw = os.getenv("CONFLUENCE_WORKSPACES", "")
    if raw.strip():
        try:
            return json.loads(raw)
        except Exception:
            pass

    workspaces = []

    # Primary workspace (your Atlassian Cloud instance)
    url = os.getenv("CONFLUENCE_URL", "https://cpp3-hpe.atlassian.net/wiki")
    email = os.getenv("CONFLUENCE_EMAIL", "")
    token = os.getenv("CONFLUENCE_API_TOKEN", "")
    space = os.getenv("CONFLUENCE_SPACE_KEY", "HPEKB")

    if url and email and token:
        workspaces.append(
            {
                "name": "HPE Engineering KB",
                "base_url": url,
                "email": email,
                "token": token,
                "space_key": space,
            }
        )

    # Public Apache Confluence (no auth required)
    workspaces.append(
        {
            "name": "Apache Wiki (Public — no auth)",
            "base_url": "https://cwiki.apache.org/confluence",
            "email": "",
            "token": "",
            "space_key": "SPARK",
            "read_only": True,  # We only READ from this, not seed
        }
    )

    return workspaces


# ─── Built-in High-Signal Articles ─────────────────────────────────

BUILTIN_ARTICLES = [
    {
        "title": "Apache Spark SQL CTE Optimizer — Known Issues and Workarounds",
        "body": """<h2>Overview</h2>
<p>The Spark SQL CTE (Common Table Expressions) optimizer contains known
issues in versions 3.3 through 3.5 related to NormalizeCTEIds and InlineCTE
passes. Bugs manifest as incorrect query results when nested CTEs reference
outer scoped aliases.</p>
<h2>Affected Components</h2>
<p>SQL Catalyst Optimizer, NormalizeCTEIds, InlineCTE, CTESubstitution,
ResolveWithCTE, AnalysisBarrier</p>
<h2>Error Patterns</h2>
<p>AnalysisException: Resolved attribute missing from child.
NullPointerException in CTESubstitution.apply().
Wrong query results when using WITH clause in nested subqueries.</p>
<h2>Root Cause</h2>
<p>NormalizeCTEIds reassigns CTE reference IDs during plan normalization.
When InlineCTE runs after NormalizeCTEIds, stale reference IDs cause
attribute resolution failures.</p>
<h2>Fix and Workaround</h2>
<p>Upgrade to Spark 3.5.1 or 4.0.0 where SPARK-45057 is resolved.
Workaround: set spark.sql.optimizer.excludedRules=
org.apache.spark.sql.catalyst.optimizer.InlineCTE</p>
<h2>Related Issues</h2>
<p>SPARK-45057, SPARK-44407, SPARK-43596</p>""",
    },
    {
        "title": "Kafka Consumer Group Rebalancing — Diagnosis and Prevention",
        "body": """<h2>Overview</h2>
<p>Kafka consumer group rebalancing occurs when partition ownership is
redistributed. Excessive rebalancing causes delays, duplicates, and
offset commit failures.</p>
<h2>Affected Components</h2>
<p>KafkaConsumer, ConsumerCoordinator, GroupCoordinator,
AbstractCoordinator, HeartbeatThread</p>
<h2>Common Causes</h2>
<p>session.timeout.ms too low. max.poll.interval.ms exceeded.
Network partition. Consumer crash during offset commit.</p>
<h2>Resolution</h2>
<p>Increase max.poll.interval.ms. Reduce max.poll.records.
Implement incremental cooperative rebalancing protocol.
Set heartbeat.interval.ms to session.timeout.ms / 3.</p>""",
    },
    {
        "title": "Kubernetes Pod OOMKilled — Root Cause Analysis and Prevention",
        "body": """<h2>Overview</h2>
<p>OOMKilled exit code 137 occurs when a container exceeds its memory limit.
The Linux kernel OOM killer terminates the process.</p>
<h2>Diagnosis</h2>
<p>kubectl describe pod: OOMKilled exit code 137.
kubectl top pod shows memory near limit before kill.</p>
<h2>Common Causes</h2>
<p>JVM heap set without accounting for off-heap memory.
Memory leak growing over time. requests.memory too low.</p>
<h2>Fix</h2>
<p>Set JVM heap to 75% of container limit: -Xmx = limit * 0.75.
Add -XX:+ExitOnOutOfMemoryError for clean exit with heap dump.
Use VPA to auto-tune limits. Set limits 20-30% above requests.</p>""",
    },
    {
        "title": "StorageController NullPointerException — Concurrent Provisioning Fix",
        "body": """<h2>Overview</h2>
<p>NullPointerException in StorageController.allocate() occurs under
concurrent VM provisioning. Missing null guard in DiskQuota.check().</p>
<h2>Stack Trace</h2>
<p>java.lang.NullPointerException at
StorageController.allocate(StorageController.java:142)
at VMProvisioningService.provision(VMProvisioningService.java:88)</p>
<h2>Root Cause</h2>
<p>StorageController.allocate() calls DiskQuota.check() which returns null
when storage pool is initializing concurrently.</p>
<h2>Fix</h2>
<p>Add null guard before dereferencing quota object.
Apply synchronized block around DiskQuota.check() call.
PR: add null guard in DiskQuota.check() for concurrent provisioning.</p>""",
    },
    {
        "title": "Spark Structured Streaming — DSv2 Checkpoint Recovery",
        "body": """<h2>Overview</h2>
<p>Structured Streaming jobs using DataSource V2 fail to recover from
checkpoints after restart when schema or partition spec changes.</p>
<h2>Error Pattern</h2>
<p>StreamingQueryException: Failed to read streaming data.
IllegalArgumentException: Incompatible schema change detected.</p>
<h2>Resolution</h2>
<p>Delete checkpoint directory and restart with clean checkpointLocation.
Enable spark.sql.streaming.forceDeleteTempCheckpointLocation=true.
For schema evolution: enable mergeSchema and upgrade to Spark 3.4+.</p>""",
    },
    {
        "title": "PySpark Connect Mode — Spark 3.4 and 3.5 Compatibility Guide",
        "body": """<h2>Overview</h2>
<p>PySpark Connect Mode provides a remote client architecture separating
driver from application code. Several compatibility issues affect migration.</p>
<h2>Common Issues</h2>
<p>SparkSession.builder.remote() fails with connection refused.
UDF registration fails silently. pandas_udf not supported in all scenarios.
Arrow serialization errors for complex types.</p>
<h2>Workaround</h2>
<p>Use serverside UDFs with spark.udf.register() through SQL.
Enable spark.sql.execution.arrow.pyspark.enabled=true.</p>""",
    },
    {
        "title": "Firefox WebGL Context Lost — Recovery and Prevention",
        "body": """<h2>Overview</h2>
<p>WebGL context loss occurs when GPU process crashes or is reset.
WEBGL_lose_context extension fires contextlost and contextrestored events.</p>
<h2>Common Causes</h2>
<p>GPU driver crash under sustained load. TDR on Windows resets GPU.
Multiple tabs consuming GPU memory. WebGL memory leak causing GPU OOM.</p>
<h2>Recovery</h2>
<p>Listen for webglcontextlost, call event.preventDefault().
Listen for webglcontextrestored to reinitialize WebGL state.
Set dom.webgl.enable-renderer-query true in about:config.</p>""",
    },
    {
        "title": "HDFS DataNode Disk Failure Recovery",
        "body": """<h2>Overview</h2>
<p>HDFS DataNode disk failures cause block unavailability and potential
data loss if replication factor is insufficient.</p>
<h2>Error Pattern</h2>
<p>IOException: Failed to move block file.
DiskChecker.DiskErrorException: directory is not writable.</p>
<h2>Resolution</h2>
<p>Check dfs.datanode.failed.volumes.tolerated setting.
Remove failed disk from dfs.datanode.data.dir config.
Run hdfs fsck to identify under-replicated blocks.
Run hdfs dfs -setrep -R to re-replicate affected data.</p>""",
    },
    {
        "title": "Apache Flink Job Manager High CPU Under Backpressure",
        "body": """<h2>Overview</h2>
<p>Flink Job Manager CPU spikes to 100% when task graph experiences
backpressure. Checkpoint coordinator repeatedly tries to trigger
checkpoints that fail.</p>
<h2>Diagnosis</h2>
<p>Flink Web UI shows High backpressure on input buffers.
Checkpoint history shows repeated failures with timeout.</p>
<h2>Resolution</h2>
<p>Increase execution.checkpointing.interval to reduce coordinator load.
Enable unaligned checkpoints.
Increase taskmanager.network.memory.fraction for network buffers.</p>""",
    },
    {
        "title": "Spark Memory Management — Executor OOM and Spill",
        "body": """<h2>Overview</h2>
<p>Spark executor OutOfMemoryError occurs when heap or off-heap memory
is exhausted. Unified memory management controls execution vs storage split.</p>
<h2>Error Patterns</h2>
<p>java.lang.OutOfMemoryError: Java heap space in executor.
GC overhead limit exceeded during shuffle.
ExecutorLostFailure due to OOM exit code 137.</p>
<h2>Resolution</h2>
<p>Increase spark.executor.memory and spark.executor.memoryOverhead.
Reduce spark.memory.storageFraction for more execution memory.
Enable off-heap: spark.memory.offHeap.enabled=true.</p>""",
    },
    {
        "title": "Kafka Producer Timeout and Retry Configuration",
        "body": """<h2>Overview</h2>
<p>Kafka producer TimeoutException and NotLeaderForPartitionException
occur under broker failures or high load.</p>
<h2>Error Pattern</h2>
<p>TimeoutException: Failed to update metadata after 60000ms.
NotLeaderOrFollowerException during leader election.</p>
<h2>Fix</h2>
<p>Set delivery.timeout.ms = request.timeout.ms + linger.ms + retries.
Enable idempotent producer: enable.idempotence=true.
Set acks=all with min.insync.replicas=2.</p>""",
    },
    {
        "title": "Thread Safety in Java Concurrent Programming",
        "body": """<h2>Overview</h2>
<p>NullPointerException and ConcurrentModificationException in Java
concurrent code result from unsynchronized access to shared mutable state.</p>
<h2>Common Anti-patterns</h2>
<p>Check-then-act without synchronization (TOCTOU race).
Double-checked locking without volatile. Iterator invalidation.</p>
<h2>Resolution</h2>
<p>Use AtomicReference for single-object updates.
Use ConcurrentHashMap instead of HashMap.
Use ReadWriteLock for read-heavy workloads.</p>""",
    },
    {
        "title": "Kubernetes Resource Limits — Preventing Scheduling Failures",
        "body": """<h2>Overview</h2>
<p>Kubernetes pods fail to schedule when resource requests exceed
available capacity or limit configurations conflict with workloads.</p>
<h2>Common Issues</h2>
<p>Pod stuck in Pending: Insufficient cpu or Insufficient memory.
Pod evicted: The node was low on resource: memory.</p>
<h2>Best Practices</h2>
<p>Set requests = average usage, limits = peak * 1.2.
Use VPA in recommendation mode. Monitor with kubectl top pods.</p>""",
    },
    {
        "title": "Apache Hive Metastore Connectivity and Schema Issues",
        "body": """<h2>Overview</h2>
<p>Hive Metastore connectivity failures block all Spark SQL operations
on Hive-managed tables.</p>
<h2>Error Patterns</h2>
<p>MetaException: Failed to get next notification.
TException: Could not connect to meta store.
Hive Schema version does not match metastore version.</p>
<h2>Resolution</h2>
<p>Run schematool -dbType mysql -upgradeSchema after Hive upgrade.
Set hive.metastore.schema.verification=false during migration.</p>""",
    },
    {
        "title": "Zookeeper Session Expiry and Leader Election Issues",
        "body": """<h2>Overview</h2>
<p>Zookeeper session expiry causes dependent services (Kafka, HBase, HDFS)
to lose coordination.</p>
<h2>Error Pattern</h2>
<p>SessionExpiredException: Session 0x... has expired.
Kafka broker lost leadership due to ZooKeeper session expiry.</p>
<h2>Resolution</h2>
<p>Increase zookeeper.session.timeout.ms to 30000+ for Kafka.
Tune JVM GC to reduce stop-the-world pauses below session timeout.</p>""",
    },
    {
        "title": "Spark Adaptive Query Execution — Configuration Guide",
        "body": """<h2>Overview</h2>
<p>AQE dynamically reoptimizes query plans at runtime based on actual stats.
Can cause unexpected plan changes affecting performance.</p>
<h2>Known Issues</h2>
<p>Broadcast join conversion causes OOM when threshold too high.
Skew detection false positives on non-uniform data.</p>
<h2>Configuration</h2>
<p>spark.sql.adaptive.enabled=true (default in Spark 3.2+).
spark.sql.adaptive.coalescePartitions.enabled=true.
spark.sql.adaptive.advisoryPartitionSizeInBytes=128m</p>""",
    },
    {
        "title": "HPE ProLiant Storage Controller — Volume Allocation Guide",
        "body": """<h2>Overview</h2>
<p>HPE ProLiant servers use Smart Array controllers for RAID management.
Volume allocation failures occur under high I/O load or concurrent
provisioning requests.</p>
<h2>Common Errors</h2>
<p>StorageController: volume allocation timeout on ProLiant DL380.
DiskQuota check returns null during concurrent provisioning.</p>
<h2>Resolution</h2>
<p>Apply HPE Smart Array firmware update.
Set concurrent provisioning limit to 4.
Configure dedicated provisioning queue thread pool size.</p>""",
    },
    {
        "title": "Network Fabric Link-State Oscillation — Diagnosis Guide",
        "body": """<h2>Overview</h2>
<p>Network link-state oscillation (flapping) causes repeated topology
changes that destabilize routing protocols.</p>
<h2>Root Causes</h2>
<p>Faulty SFP transceiver or cable. MTU mismatch.
Auto-negotiation failure. LACP timeout under heavy multicast traffic.</p>
<h2>Resolution</h2>
<p>Enable port dampening to suppress flapping interfaces.
Replace suspect SFP modules. Force speed and duplex settings.
Increase LACP timeout from short (1s) to long (30s) mode.</p>""",
    },
    {
        "title": "VS Code Extension High Memory Usage and Language Server Crashes",
        "body": """<h2>Overview</h2>
<p>VS Code extensions, particularly TypeScript and Python language servers,
consume excessive memory on large projects.</p>
<h2>Symptoms</h2>
<p>TypeScript server crash with signal SIGTERM.
Extension host terminated unexpectedly.
Pylance: not enough memory to resolve imports.</p>
<h2>Resolution</h2>
<p>Set typescript.tsserver.maxTsServerMemory: 4096.
Exclude node_modules from TypeScript project.
Use pyrightconfig.json to limit Pylance analysis scope.</p>""",
    },
    {
        "title": "Kafka Log Compaction — Configuration and Troubleshooting",
        "body": """<h2>Overview</h2>
<p>Kafka log compaction retains the last value per key. Compaction failures
leave tombstone records and cause consumer lag to grow.</p>
<h2>Error Pattern</h2>
<p>LogCleaningException: kafka.log.LogCleaner error.
Consumer position falls outside compacted offsets.</p>
<h2>Resolution</h2>
<p>Set log.cleaner.io.max.bytes.per.second to limit cleaner I/O.
Increase log.cleaner.threads if compaction falls behind.
Set delete.retention.ms to keep tombstones for consumers.</p>""",
    },
    {
        "title": "Java Heap Dump Analysis — OutOfMemoryError Investigation",
        "body": """<h2>Overview</h2>
<p>OutOfMemoryError in Java requires heap dump analysis to identify leaks.
Common patterns: unbounded caches, listener accumulation, classloader leaks.</p>
<h2>Generating Heap Dump</h2>
<p>JVM flags: -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/heap.hprof
Use jmap: jmap -dump:format=b,file=heap.hprof pid</p>
<h2>Common Leak Patterns</h2>
<p>Static HashMap accumulating entries without eviction.
ThreadLocal variables not cleaned up after thread pool reuse.</p>""",
    },
    {
        "title": "Spark SQL Join Strategy Selection and Broadcast Hints",
        "body": """<h2>Overview</h2>
<p>Spark SQL join strategy selection significantly impacts performance.
Incorrect strategy causes OOM or excessive shuffle.</p>
<h2>Broadcast Join OOM</h2>
<p>SparkException: Cannot broadcast table larger than 8GB.
java.lang.OutOfMemoryError during broadcast exchange.</p>
<h2>Hints</h2>
<p>SELECT /*+ BROADCAST(small_table) */ ...
Reduce spark.sql.autoBroadcastJoinThreshold or disable with -1.</p>""",
    },
    {
        "title": "Apache Cassandra Read Timeout and Coordinator Issues",
        "body": """<h2>Overview</h2>
<p>ReadTimeoutException occurs when coordinator does not receive responses
from replicas within read_request_timeout_in_ms.</p>
<h2>Diagnosis</h2>
<p>nodetool tpstats shows dropped messages increasing.
ReadTimeoutException: code=0x1200 in coordinator logs.</p>
<h2>Resolution</h2>
<p>Run nodetool repair to fix inconsistent replica state.
Tune read_request_timeout_in_ms from 5000 to 10000.
Compact tables with high tombstone counts using nodetool compact.</p>""",
    },
    {
        "title": "Apache Airflow Task Failure Recovery and DAG Scheduling",
        "body": """<h2>Overview</h2>
<p>Airflow DAG scheduling failures cause pipeline delays. Common issues:
scheduler heartbeat failures, zombie tasks, executor saturation.</p>
<h2>Error Pattern</h2>
<p>Scheduler heartbeat failed. Zombie task detected.
DAGRun stuck in running state with no active tasks.</p>
<h2>Resolution</h2>
<p>Set core.dag_file_processor_timeout = 120 for complex DAGs.
Set task retries and retry_delay for transient failures.</p>""",
    },
]


# ─── Markdown Scanner ──────────────────────────────────────────────


def load_markdown_articles() -> list[dict]:
    kb_dir = os.getenv("KB_MARKDOWN_DIRECTORY", "./knowledge_base")
    kb_path = Path(kb_dir)
    if not kb_path.exists():
        return []

    articles = []
    for md_file in kb_path.rglob("*.md"):
        try:
            text = md_file.read_text(encoding="utf-8")
            # Extract title from first H1 heading
            h1_match = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
            if h1_match:
                title = h1_match.group(1).strip()
                # Remove the H1 line from body
                body_md = text[h1_match.end() :].strip()
            else:
                title = md_file.stem.replace("-", " ").title()
                body_md = text

            # Convert Markdown to Confluence storage HTML
            html = _markdown_to_html(body_md)
            articles.append({"title": title, "body": html})
            print(f"  Loaded markdown: {title}")
        except Exception as e:
            print(f"  Warning: Could not read {md_file}: {e}")
    return articles


def _markdown_to_html(md: str) -> str:
    """Convert basic Markdown to Confluence storage HTML."""
    html = md
    # H2 headings
    html = re.sub(r"^##\s+(.+)$", r"<h2>\1</h2>", html, flags=re.MULTILINE)
    # H3 headings
    html = re.sub(r"^###\s+(.+)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)
    # Bold text
    html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)
    # Italic
    html = re.sub(r"\*(.+?)\*", r"<em>\1</em>", html)
    # Code blocks
    html = re.sub(r"`(.+?)`", r"<code>\1</code>", html)
    # Paragraphs (double newline)
    blocks = re.split(r"\n\n+", html)
    result = []
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        if block.startswith("<h") or block.startswith("<ul"):
            result.append(block)
        else:
            result.append(f"<p>{block}</p>")
    return "\n".join(result)


# ─── HTTP Helpers ───────────────────────────────────────────────────


def _make_headers(workspace: dict) -> dict:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    email = workspace.get("email", "").strip()
    token = workspace.get("token", "").strip()

    if email and token:
        creds = base64.b64encode(f"{email}:{token}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"
    elif token:
        headers["Authorization"] = f"Bearer {token}"
    # else: public wiki — no auth header
    return headers


async def get_existing_titles(workspace: dict) -> set[str]:
    base = workspace["base_url"].rstrip("/")
    space = workspace["space_key"]
    url = f"{base}/rest/api/content/search"
    params = {
        "cql": f'space = "{space}" AND type = page',
        "limit": 200,
    }
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(
                url, headers=_make_headers(workspace), params=params
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                return {r["title"] for r in results}
    except Exception as e:
        print(f"  Warning: Could not fetch titles: {e}")
    return set()


async def create_article(workspace: dict, title: str, body: str) -> bool:
    base = workspace["base_url"].rstrip("/")
    space = workspace["space_key"]
    url = f"{base}/rest/api/content"
    payload = {
        "type": "page",
        "title": title,
        "space": {"key": space},
        "body": {
            "storage": {
                "value": body.strip(),
                "representation": "storage",
            }
        },
    }
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.post(
                url, headers=_make_headers(workspace), json=payload
            )
            if resp.status_code in (200, 201):
                print(f"  ✓ Created: {title}")
                return True
            else:
                print(f"  ✗ Failed ({resp.status_code}): " f"{title[:60]}")
                if resp.status_code != 400:
                    print(f"    {resp.text[:150]}")
                return False
    except Exception as e:
        print(f"  ✗ Error for '{title[:60]}': {e}")
        return False


# ─── Seeding Engine ────────────────────────────────────────────────


async def seed_workspace(workspace: dict, articles: list[dict]) -> None:
    name = workspace.get("name", workspace["base_url"])
    is_readonly = workspace.get("read_only", False)

    print(f"\n{'='*60}")
    print(f"Workspace: {name}")
    print(f"Space:     {workspace['space_key']}")
    print(
        f"Auth:      "
        f"{'Public (no auth)' if not workspace.get('token') else 'Authenticated'}"
    )

    if is_readonly:
        print("Mode: READ ONLY — skipping seeding for public wiki")
        print(f"{'='*60}")
        return

    print(f"Articles:  {len(articles)} to process")
    print(f"{'='*60}")

    existing = await get_existing_titles(workspace)
    print(f"Existing articles in space: {len(existing)}")
    print()

    created = skipped = failed = 0

    for article in articles:
        title = article["title"]
        if title in existing:
            print(f"  ~ Skip: {title[:70]}")
            skipped += 1
            continue
        ok = await create_article(workspace, title, article["body"])
        if ok:
            created += 1
        else:
            failed += 1
        # Rate limiting delay
        await asyncio.sleep(0.3)

    print()
    print(f"Result: {created} created, " f"{skipped} skipped, {failed} failed")


async def main() -> None:
    print("Agentic Bug Triage — Confluence Knowledge Base Seeder")
    print("=" * 60)

    workspaces = load_workspaces()
    if not workspaces:
        print("ERROR: No workspaces configured.")
        print("Set CONFLUENCE_EMAIL and CONFLUENCE_API_TOKEN " "in .env")
        return

    # Load articles
    articles = list(BUILTIN_ARTICLES)

    # Load local markdown files
    md_articles = load_markdown_articles()
    if md_articles:
        print(f"Loaded {len(md_articles)} local markdown articles")
        articles.extend(md_articles)

    print(f"Total articles to seed: {len(articles)}")

    # Seed all workspaces concurrently
    await asyncio.gather(*[seed_workspace(ws, articles) for ws in workspaces])

    print("\n" + "=" * 60)
    print("Seeding complete.")
    print()
    print("Next steps:")
    print("1. Verify articles in your Confluence space")
    print("2. Add public Apache wikis via Token Settings:")
    print("   Name: Apache Spark Wiki")
    print("   URL:  https://cwiki.apache.org/confluence")
    print("   Type: confluence")
    print("   Space: SPARK (no token required)")
    print("   ---")
    print("   Name: Apache Kafka Wiki")
    print("   URL:  https://cwiki.apache.org/confluence")
    print("   Type: confluence")
    print("   Space: KAFKA (no token required)")


if __name__ == "__main__":
    asyncio.run(main())
