import asyncio
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from orchestrator.db.base import Base
from orchestrator.db.session import engine, AsyncSessionLocal, ensure_runtime_schema
from orchestrator.db.models import (
    Base,
    SourceRegistry,
    UserRole,
    SystemGroupRegistry,
    BugGroupMapping,
)
from sqlalchemy import select

DEMO_SOURCES = [
    {
        "source_id": "apache-spark-github",
        "display_name": "Apache Spark (GitHub)",
        "system_type": "github",
        "base_url": "https://api.github.com",
        "auth_type": "bearer_token",
        "auth_secret_ref": "APACHE_SPARK_GITHUB_TOKEN",
        "project_key": "apache/spark",
        "ticket_prefix": "SGH",
        "enabled": True,
    },
    {
        "source_id": "apache-spark-jira",
        "display_name": "Apache Spark (JIRA)",
        "system_type": "jira_apache",
        "base_url": "https://issues.apache.org/jira",
        "auth_type": "bearer_token",
        "auth_secret_ref": "APACHE_SPARK_JIRA_TOKEN",
        "project_key": "SPARK",
        "ticket_prefix": "SPARK",
        "enabled": True,
    },
    {
        "source_id": "apache-kafka-github",
        "display_name": "Apache Kafka (GitHub)",
        "system_type": "github",
        "base_url": "https://api.github.com",
        "auth_type": "bearer_token",
        "auth_secret_ref": "APACHE_KAFKA_GITHUB_TOKEN",
        "project_key": "apache/kafka",
        "ticket_prefix": "KGH",
        "enabled": False,
    },
    {
        "source_id": "apache-kafka-jira",
        "display_name": "Apache Kafka (JIRA)",
        "system_type": "jira_apache",
        "base_url": "https://issues.apache.org/jira",
        "auth_type": "bearer_token",
        "auth_secret_ref": "APACHE_KAFKA_JIRA_TOKEN",
        "project_key": "KAFKA",
        "ticket_prefix": "KAFKA",
        "enabled": False,
    },
    {
        "source_id": "mozilla-firefox-bugzilla",
        "display_name": "Mozilla Firefox (Bugzilla)",
        "system_type": "bugzilla",
        "base_url": "https://bugzilla.mozilla.org",
        "auth_type": "bearer_token",
        "auth_secret_ref": "MOZILLA_FIREFOX_BUGZILLA_TOKEN",
        "project_key": "Firefox",
        "ticket_prefix": "BUG",
        "enabled": False,
    },
    {
        "source_id": "kubernetes-github",
        "display_name": "Kubernetes — GitHub Issues",
        "system_type": "github",
        "base_url": "https://api.github.com",
        "auth_type": "pat",
        "auth_secret_ref": "APACHE_SPARK_GITHUB_TOKEN",
        "project_key": "kubernetes/kubernetes",
        "ticket_prefix": "K8S",
        "enabled": False,
    },
    {
        "source_id": "hpe-confluence",
        "display_name": "HPE Engineering KB (Confluence)",
        "system_type": "confluence",
        "base_url": "https://cpp3-hpe.atlassian.net/wiki",
        "auth_type": "basic",
        "auth_secret_ref": "CONFLUENCE_API_TOKEN",
        "project_key": "HPEKB",
        "ticket_prefix": "CONF",
        "enabled": False,
    },
]

DEMO_USERS = [
    {
        "email": "disha@hpe.com",
        "password": "password123",
        "role": "engineer",
        "display_name": "Disha Jain",
    },
    {
        "email": "anuj@hpe.com",
        "password": "password123",
        "role": "engineer",
        "display_name": "Anuj Modani",
    },
    {
        "email": "shivansh@hpe.com",
        "password": "password123",
        "role": "engineer",
        "display_name": "Shivansh Gaur",
    },
    {
        "email": "pulkit@hpe.com",
        "password": "password123",
        "role": "engineer",
        "display_name": "Pulkit Jain",
    },
    {
        "email": "om@hpe.com",
        "password": "password123",
        "role": "engineer",
        "display_name": "Om",
    },
    {
        "email": "admin@hpe.com",
        "password": "admin123",
        "role": "admin",
        "display_name": "Admin User",
    },
    {
        "email": "customer@acme.com",
        "password": "customer123",
        "role": "customer",
        "display_name": "Acme Customer",
    },
    {
        "email": "exec@hpe.com",
        "password": "exec123",
        "role": "executive",
        "display_name": "HPE Executive",
    }
]
async def init():
    print("Creating database tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await ensure_runtime_schema()
    print("Tables created.")

    async with AsyncSessionLocal() as db:
        print("\nSeeding data sources...")
        for src_data in DEMO_SOURCES:
            existing = await db.execute(
                select(SourceRegistry).where(
                    SourceRegistry.source_id == src_data["source_id"]
                )
            )
            if existing.scalar_one_or_none() is None:
                db.add(SourceRegistry(**src_data))
                print(f"  + {src_data['source_id']}")
            else:
                print(f"  ~ {src_data['source_id']} (already exists)")
        await db.commit()

        print("\nSeeding demo users...")
        from passlib.context import CryptContext

        pwd_ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
        user_count_result = await db.execute(select(UserRole))
        existing_users = list(user_count_result.scalars().all())
        if not existing_users:
            for u in DEMO_USERS:
                user = UserRole(
                    user_id=u["email"],
                    role=u["role"],
                    password_hash=pwd_ctx.hash(u["password"]),
                    display_name=u["display_name"],
                )
                db.add(user)
                print(f"  + {u['email']} ({u['role']})")
            await db.commit()
        else:
            print(f"  ~ {len(existing_users)} users already exist, skipping seed")

    print("\nDatabase initialization complete.")


if __name__ == "__main__":
    asyncio.run(init())
