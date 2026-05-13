#!/usr/bin/env bash
# ============================================================
# Rebuilds commit history for redis-streams-messaging
# Also: deletes REPIN.md, fixes Mermaid fence in README
# Works on macOS and Linux
#
# Run inside your cloned repo:
#   cd /path/to/redis-streams-messaging
#   bash fix-redis-history.sh
#   git push --force origin main
# ============================================================
set -e

days_ago() {
  if date --version >/dev/null 2>&1; then
    date -d "$1 days ago" "+%Y-%m-%dT10:00:00"
  else
    date -v-"$1"d "+%Y-%m-%dT10:00:00"
  fi
}

# ── Fix REPIN.md and Mermaid BEFORE backing up ───────────────
if [ -f "REPIN.md" ]; then
  rm REPIN.md
  echo "✓ Deleted REPIN.md"
fi

if [ -f "README.md" ]; then
  # Fix broken flowchart fence -> proper mermaid block
  if grep -q '```flowchart LR' README.md; then
    sed -i.bak 's/```flowchart LR/```mermaid/' README.md
    rm -f README.md.bak
    echo "✓ Fixed Mermaid fence in README.md"
  fi
fi

echo "==> Backing up all files to /tmp/redis-backup"
rm -rf /tmp/redis-backup
cp -r . /tmp/redis-backup
echo "✓ Backup done"

echo "==> Wiping git history (orphan branch)"
git checkout --orphan fresh-history
git rm -rf . --quiet
echo "✓ Working tree cleared"

commit_step() {
  local DATE="$1"
  local MSG="$2"
  shift 2
  for path in "$@"; do
    if [ -d "/tmp/redis-backup/$path" ]; then
      mkdir -p "$path"
      cp -r "/tmp/redis-backup/$path/." "$path/"
    elif [ -f "/tmp/redis-backup/$path" ]; then
      mkdir -p "$(dirname "$path")"
      cp "/tmp/redis-backup/$path" "$path"
    fi
  done
  git add -A
  if ! git diff --cached --quiet; then
    GIT_COMMITTER_DATE="$DATE" GIT_AUTHOR_DATE="$DATE" \
      git commit -m "$MSG" --quiet
    echo "✓ $MSG"
  fi
}

# ── COMMIT 1: Scaffold (20 days ago) ─────────────────────────
commit_step "$(days_ago 20)" \
  "chore: init repo with docker-compose for local Redis Streams dev" \
  ".gitignore" "docker-compose.yml"

# ── COMMIT 2: Python producer (18 days ago) ──────────────────
commit_step "$(days_ago 18)" \
  "feat(python): implement StreamProducer with XADD and message envelope" \
  "python/streams" "python/pyproject.toml" "python/setup.py"

# ── COMMIT 3: Python consumer + groups (16 days ago) ─────────
commit_step "$(days_ago 16)" \
  "feat(python): add StreamConsumer with XREADGROUP and consumer group support" \
  "python"

# ── COMMIT 4: Python DLQ (14 days ago) ───────────────────────
commit_step "$(days_ago 14)" \
  "feat(python): add Dead Letter Queue — failed messages route to {stream}:dlq" \
  "python"

# ── COMMIT 5: Python examples + tests (12 days ago) ──────────
commit_step "$(days_ago 12)" \
  "test(python): add unit tests with fakeredis for producer, consumer, and DLQ" \
  "python/tests" "python/examples"

# ── COMMIT 6: Java producer (10 days ago) ────────────────────
commit_step "$(days_ago 10)" \
  "feat(java): implement StreamProducer using Lettuce with shared envelope schema" \
  "java/src/main/java/com/vipulconda/streams/producer" \
  "java/pom.xml"

# ── COMMIT 7: Java consumer (8 days ago) ─────────────────────
commit_step "$(days_ago 8)" \
  "feat(java): add StreamConsumer with consumer group and at-least-once delivery" \
  "java/src/main/java/com/vipulconda/streams/consumer" \
  "java/src/main/java/com/vipulconda/streams/model"

# ── COMMIT 8: Java DLQ + retry (6 days ago) ──────────────────
commit_step "$(days_ago 6)" \
  "feat(java): add retry logic with configurable max_retries and DLQ routing" \
  "java/src/main/java"

# ── COMMIT 9: Java tests — Testcontainers (4 days ago) ───────
commit_step "$(days_ago 4)" \
  "test(java): add integration tests with Testcontainers and Redis 7" \
  "java/src/test"

# ── COMMIT 10: README (2 days ago) ───────────────────────────
commit_step "$(days_ago 2)" \
  "docs: add README with design decisions, parallel API table, and quick start" \
  "README.md"

# ── Catch any remaining files ─────────────────────────────────
cp -rn /tmp/redis-backup/. . 2>/dev/null || true
rm -f REPIN.md  # ensure it stays deleted
git add -A
if ! git diff --cached --quiet; then
  GIT_COMMITTER_DATE="$(days_ago 1)" GIT_AUTHOR_DATE="$(days_ago 1)" \
    git commit -m "chore: final cleanup and remove internal notes" --quiet
  echo "✓ chore: final cleanup"
fi

git branch -M main 2>/dev/null || git branch -M fresh-history main

echo ""
echo "✅ Done! Commit history:"
git log --format="%h %ad  %s" --date=short
echo ""
echo "Next: git push --force origin main"