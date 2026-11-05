# Remote cache prompt template

Every work item's prompt for the remote cache work
([zerobull-consulting/dagger#1](https://github.com/zerobull-consulting/dagger/issues/1))
is generated from this file. It holds the template, the conventions for the
whole prompt, and the working agreement for review and merge (section 12).
It is the process's source, and it changes by PR (see
[Changing this file](#changing-this-file)).

**How the pieces fit:**

- **Issues stay general:** what and why, readable by a human or an agent
  (like [zerobull-consulting/dagger#63](https://github.com/zerobull-consulting/dagger/issues/63),
  [zerobull-consulting/dagger#65](https://github.com/zerobull-consulting/dagger/issues/65),
  [zerobull-consulting/dagger#94](https://github.com/zerobull-consulting/dagger/issues/94)).
- **The full prompt goes in the PR it started,** verbatim, in a collapsed
  block. It's the record of how that execution was instructed.
- **The template is how prompts are generated,** and where the process
  improves.

## The template

Keep the sections in this order, leave out a section only when it would be
empty, and keep each section's purpose in mind.

### 1. Header

Repository, the issue or issues it works on, the tracking issue, where it sits
in the plan, and *why it matters now*.

*Why:* the agent weighs trade-offs better when it knows what the work is for.

### 2. Before starting, read

The issues (with which comments), docs (with section names) and code (with
function names) the work touches, and related issues it mustn't redo or
contradict.

*Why:* grounding in the real code beats a summary of it, and naming the
overlaps prevents duplicate fixes.

### 3. Prerequisites

When any exist: what must be true before work starts, and to stop and ask if
it isn't.

*Why:* stops the agent working around a missing secret or setup.

### 4. Branch and PR

- Branch name and base; open as a draft.
- The closing-keyword caveat: keywords don't fire on feature branches, so
  close by hand.
- Full `owner/repo#N` references, since a bare `#N` can link to the upstream
  repository.
- **The prompt goes in the PR, verbatim:** at the end of the PR description,
  under a "Prompt" heading, inside
  `<details><summary>The prompt that started this PR</summary>` …
  `</details>`. Follow the repository's PR template, if it has one, for
  everything above it. A follow-up PR from the same prompt links to the
  original's instead of copying it.
- **The prompt stays out of the commit history:** set the squash commit's
  title and message explicitly. The title is the PR's title with its full
  reference, like `(zerobull-consulting/dagger#97)`, never GitHub's default
  `(#97)`; the message is the PR's summary, without the prompt. Section 12,
  step 5 says how.

*Why:* repository quirks that bit earlier rounds, and a record of exactly what
produced each PR.

### 5. Design

Each point marked **decided** (the requester's) or **proposed, confirm or
change in the PR** (the agent's to judge), and "write this into the PR before
building on it".

*Why:* separates the requester's decisions from the agent's, and puts the
reasoning on the record before code.

### 6. What to build

A numbered list of concrete changes.

*Why:* a checklist the PR can be checked against.

### 7. Out of scope

What's excluded, and which issue owns it.

*Why:* stops scope creep, and keeps neighbouring issues coherent.

### 8. Tests

By layer (fake engine, MinIO, built binary, real engines), with the exact
commands that must pass, and "prove each new test can fail by breaking what it
guards".

*Why:* every layer has caught bugs the others missed.

### 9. Rules

Invariants:

- no engine changes without stopping first;
- never delete on an engine's signal;
- bound everything;
- no engine- or bucket-supplied strings in labels, keys or file names without
  validating them;
- known-gap tests plus issues for confirmed gaps not fixed; unconfirmed leads
  go in one issue, with no test;
- stage files explicitly, and never commit built binaries or `dagger.lock`
  changes by accident.

*Why:* the lines that must never move, whatever the task.

### 10. Environment notes

The sandbox's quirks: the Docker Hub mirror, the TLS CA bundle, the unsampled
`TRACEPARENT`, and which buckets not to touch.

*Why:* each one cost a round once.

### 11. Lessons from earlier rounds

Only the ones relevant to this task.

*Why:* that's where process improvements go first, before they become rules.

### 12. Review and merge

In the prompt, the working agreement's points; the agent follows the
agreement's full text below:

- open as a draft, and get CI green;
- decide on splitting once, from the first diff's lines added plus deleted;
- run `/code-review high` against `origin/feature/remote-cache` and fix the
  real findings, then mark the PR ready;
- one push per round, at most four review rounds;
- then known-gap tests and issues for confirmed findings, one issue of
  unverified leads for the rest, one final push, then the squash merge with
  its explicit message;
- the exception for deleting data, publishing unverified blobs or bundles, or
  exposing a token.

*Why:* fewer, more useful review rounds.

#### The working agreement: Copilot review rounds

This is the agreement's current text. It was first posted on
zerobull-consulting/dagger#1 as
[the agreement](https://github.com/zerobull-consulting/dagger/issues/1#issuecomment-6639444472)
and [its amendment](https://github.com/zerobull-consulting/dagger/issues/1#issuecomment-6651252824)
(four rounds for every PR); this file supersedes both.

**Why:** large PRs (zerobull-consulting/dagger#56: 16 Copilot reviews;
zerobull-consulting/dagger#71: 6) kept collecting 🔵 "Needs a closer look"
reviews and never reached a clean one. With draft-first, a self-review before
marking ready, and one push per round, reviews stopped multiplying by the
number of commits: zerobull-consulting/dagger#76 (321 lines) took 3 Copilot
reviews, zerobull-consulting/dagger#78 (1,543) took 2, and
zerobull-consulting/dagger#79 (1,238) took 5, which is four rounds plus the
final push's review. The cap is what bounds the cost, and splitting large work
keeps the rounds productive.

**How this repository is set up:**

- An org ruleset has Copilot review **every push** to a ready PR, and not
  drafts. So each push is a review round.
- `feature/remote-cache` requires `unit tests`, `S3 tests against MinIO` and
  `real engines (e2e)`. Copilot is not a required check, so the merge waits on
  those checks only.

**The limit:**

- Every PR gets at most **four** review rounds (reviews 1 to 4), whatever its
  size.
- **Decide on splitting once, from the first diff:** when the draft first does
  what the prompt asks, before `/code-review` or any other review. Its size is
  the lines added plus the lines deleted, counting every file (tests, docs and
  generated files included). A PR that adds 2,500 lines and deletes 500 counts
  as 3,000. The command compares the base with the committed `HEAD`, so
  commit the draft first. A binary file shows as `-` and counts as 0:

  ```bash
  git fetch origin feature/remote-cache &&
    git diff --numstat origin/feature/remote-cache...HEAD |
    awk '{n += $1 + $2} END {print n}'
  ```

- Over about 2,500 lines, split it before it's marked ready. Put the size in
  the PR description, and if a PR over it isn't split, say why.
- Don't revisit the decision later: fixes during the rounds never reopen it.
  The cap on review rounds is what limits how big a PR gets after that.
- Split large work into smaller PRs where it splits naturally. That cuts
  findings more than anything else.

**The flow:**

1. **Open the PR as a draft.** Push as often as you need; Copilot doesn't
   review drafts.
2. **Before marking it ready:**
   - Measure the first diff and decide on splitting, once (see the limit
     above).
   - CI is green on the draft, and the PR's tests have passed locally.
   - Run `/code-review high` on the branch against
     `origin/feature/remote-cache` and fix the real findings.
   - Then mark the PR ready. That starts **review 1**.
3. **Each fix round:**
   - Trace every finding: red, yellow, blue, and any you can read only in its
     session.
   - Fix the real ones, each with a test that fails without the fix.
   - Answer the others on their thread with what you traced, and resolve the
     threads you've handled.
   - Push all of it in **one push**. Gather everything into that push: no
     README touch-ups, rebases or follow-up commits in between.
4. **After the last round** (review 4), no more fix rounds. Handle whatever
   that review reports (new findings, a 🔵 "Needs a closer look" with or
   without text, findings only in its session) like this:
   - **Confirmed** (you traced a real path to the failure): pin it with a
     known-gap test whose comment links the issue, and file an issue under
     zerobull-consulting/dagger#1.
   - **Not confirmed, or text you can't read:** list them in one issue under
     zerobull-consulting/dagger#1 as unverified leads (file, lines, type),
     with no test.
     Don't ask for Copilot's session findings.
   - Answer each thread with its issue link, and resolve it.
   - **The one exception:** fix it with a test in this same push, instead of
     filing it, if a confirmed finding means the PR deletes data it shouldn't,
     publishes an unverified blob or bundle, or exposes a token. If that fix
     needs a design decision, don't merge: stop and report.
5. **Merge:** push those known-gap tests and fixes as one final push. **Then**
   squash-merge, with the title and message set as in section 4:
   - where the auto-merge tool can set the title and message, enable
     auto-merge (squash) with them;
   - where it can't, wait for the required checks on the final push to pass,
     then squash-merge with the explicit title and message, pinned to the head
     commit.

   Enabling auto-merge before the push could merge the old head the moment its
   checks are green. The checks gate the merge; Copilot's review of that push
   doesn't.
6. **CI red at any point:** fix it at the root cause; never retry or loosen a
   check. After the last round, the reviews these pushes start are handled
   like the final push's.
7. **After the merge:**
   - If Copilot reviewed the final push, file anything new in it as issues
     under zerobull-consulting/dagger#1. No new PR.
   - Close the PR's issue by hand, and unsubscribe.
   - Report how many review rounds were used, what was fixed, and which
     issues were filed.

**Stop instead of merging** only when CI stays red after a genuine fix, or for
the exception in step 4. Copilot's verdict alone doesn't block a merge once
the round limit is reached.

### 13. After the merge

Issues to close by hand, comments to post and where, follow-up issues to file,
and what to report.

*Why:* keeps the tracking issue and the issues that depend on this one current
without the requester doing it.

### 14. Done when

Observable outcomes, each shown by a test or a run.

*Why:* an end the agent can check for itself.

## Conventions for the whole prompt

- Say when to **stop and ask** rather than decide: design decisions, a missing
  prerequisite, anything irreversible.
- **Measure before choosing a number,** and put the measurement in the PR.
- End the prompt with a line naming the template version it was generated
  from:

  > Generated from `hack/remote-cache/PROMPT_TEMPLATE.md` at `<commit>`

  where `<commit>` is the commit of `feature/remote-cache` that last changed
  this file:

  ```bash
  git fetch origin feature/remote-cache &&
    git log -1 --format=%h origin/feature/remote-cache -- hack/remote-cache/PROMPT_TEMPLATE.md
  ```

## Starting a session

Paste the full prompt. The agent copies it, verbatim, into the PR it opens:

```markdown
## Prompt

<details><summary>The prompt that started this PR</summary>

(the prompt, verbatim)

</details>
```

Keep the blank lines around the prompt inside `<details>`, so its Markdown
renders.

## Changing this file

- The template changes by PR, like any other file, into `feature/remote-cache`.
- The PR says what changed in the process and why.
- The next prompt generated names the new commit (see the last convention
  above), so every prompt shows which version of the process it followed.
- The draft in zerobull-consulting/dagger#95's body and the working
  agreement's comments on zerobull-consulting/dagger#1 are history; this file
  supersedes them.
