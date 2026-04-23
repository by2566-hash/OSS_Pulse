from __future__ import annotations

from pyspark.sql.types import ArrayType, BooleanType, LongType, StringType, StructField, StructType


commit_author_schema = StructType(
    [
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

commit_schema = StructType(
    [
        StructField("sha", StringType(), True),
        StructField("author", commit_author_schema, True),
        StructField("message", StringType(), True),
        StructField("distinct", BooleanType(), True),
        StructField("url", StringType(), True),
    ]
)

user_schema = StructType(
    [
        StructField("login", StringType(), True),
        StructField("id", LongType(), True),
    ]
)

issue_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("number", LongType(), True),
        StructField("title", StringType(), True),
        StructField("state", StringType(), True),
        StructField("user", user_schema, True),
    ]
)

pull_request_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("number", LongType(), True),
        StructField("state", StringType(), True),
        StructField("merged", BooleanType(), True),
        StructField("title", StringType(), True),
        StructField("user", user_schema, True),
    ]
)

forkee_owner_schema = StructType(
    [
        StructField("login", StringType(), True),
        StructField("id", LongType(), True),
    ]
)

forkee_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("full_name", StringType(), True),
        StructField("owner", forkee_owner_schema, True),
    ]
)

payload_schema = StructType(
    [
        StructField("action", StringType(), True),
        StructField("ref", StringType(), True),
        StructField("ref_type", StringType(), True),
        StructField("master_branch", StringType(), True),
        StructField("description", StringType(), True),
        StructField("number", LongType(), True),
        StructField("size", LongType(), True),
        StructField("distinct_size", LongType(), True),
        StructField("push_id", LongType(), True),
        StructField("commits", ArrayType(commit_schema), True),
        StructField("issue", issue_schema, True),
        StructField("pull_request", pull_request_schema, True),
        StructField("forkee", forkee_schema, True),
    ]
)

actor_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("display_login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]
)

repo_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

org_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]
)

gharchive_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("actor", actor_schema, True),
        StructField("repo", repo_schema, True),
        StructField("payload", payload_schema, True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True),
        StructField("org", org_schema, True),
    ]
)

CORE_EVENT_TYPES = [
    "WatchEvent",
    "ForkEvent",
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
]
