# Remote cache prompt template

**Version:** [zerobull-consulting/dagger#191](https://github.com/zerobull-consulting/dagger/pull/191)

Every work item's prompt for the remote cache work is generated from this file. It holds the template, the conventions for the
whole prompt, and the working agreement for review and merge (section 12).
It is the process's source, and it changes by PR (see
[Changing this file](#changing-this-file)).

**Tracking issues.** zerobull-consulting/dagger#1, the original tracking
issue, is closed. Each prompt names its item's own tracking issue (for
example zerobull-consulting/dagger#117 or zerobull-consulting/dagger#139, or
the item's issue itself when it has none). Wherever this file says "the
tracking issue", it means that one.

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

### 1a. Running alongside

When the item runs in parallel with others: each sibling session's ID and
title, the issue it works on, and the files and sections it owns. Then: stay
in your own files; when `feature/remote-cache` moves, merge it in, never
rebase (force-pushes are blocked); and whoever merges second resolves the
conflict, keeping both changes.

*Why:* every parallel round so far needed this, written by hand, and
sessions had to be sent each other's IDs after they started.

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

- no engine changes without stopping first (the `fork paths` check,
  zerobull-consulting/dagger#141, also fails any change outside the
  service's paths);
- **verify by running:** a claim about the engine's, GitHub's or a store's
  behaviour goes into an issue, a doc or the code only after a real run shows
  it, not from reading code (zerobull-consulting/dagger#127 corrected a
  code-reading claim that a real kept-volume run disproved);
- never delete on an engine's signal;
- bound everything;
- no engine- or bucket-supplied strings in labels, keys or file names without
  validating them;
- known-gap tests plus issues for confirmed gaps not fixed; unconfirmed leads
  are appended to the tracking issue's rolling leads issue (see section 12),
  with no test;
- stage files explicitly, and never commit built binaries or `dagger.lock`
  changes by accident.

*Why:* the lines that must never move, whatever the task.

**Sections 9, 10 and 12 may be given by reference.** A prompt can say
"Rules, environment notes and review: sections 9, 10 and 12 of
`hack/remote-cache/PROMPT_TEMPLATE.md`, as of the version named at the end",
and add only what differs for the item (an extra rule, an environment quirk
it will hit). The agent reads the file. Copying
them into every prompt cost about 3k characters each, and the copies drifted.

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
- at most **two** Copilot reviews in all, counting every push's: review 1,
  one fix push, review 2, and **no push after review 2**;
- a second `/code-review high` after review 1, fixed in that same fix push;
- never end a turn waiting on Copilot or CI without a check-in scheduled;
- review 2's confirmed findings become issues, the rest go to the tracking
  issue's rolling leads issue, then the squash merge of review 2's head, or
  the head of a push step 4's exception or step 6's CI fix allowed after it,
  once the ruleset's required checks have passed on that head, with its
  explicit message. The session merges it itself, unless the prompt says the
  user merges, or an orchestrating session started it: then the session
  sends a "ready to merge" report and the orchestrator merges;
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
- `feature/remote-cache`'s ruleset is the source of truth for required checks.
  Read them from the ruleset, not from this file and not from a PR's checks
  list, which also shows optional checks. Use the PR's checks only to confirm
  that each required one passed on the head being merged. Copilot is not a
  required check, so the merge waits on those checks only.

**The limit:**

- Every PR gets at most **two** Copilot reviews (reviews 1 and 2), whatever
  its size or what it touches. **Every push to a ready PR counts,** so there
  is exactly one fix push, and nothing is pushed after review 2.
  - *Why the final push counts:* the earlier text capped "rounds" but required
    a final push after the last one, so a cap of four meant five reviews
    (zerobull-consulting/dagger#121 and #141 had five). Counting every
    review makes the cap what it says.
  - *Why two:* under the earlier cap of four, most PRs from
    zerobull-consulting/dagger#107 to #168 already finished in two, thanks to
    the self-review before marking ready. zerobull-consulting/dagger#168 met
    every target under a cap of two.
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
     `origin/feature/remote-cache` and fix the real findings. Give it the
     range explicitly (`origin/feature/remote-cache...HEAD`, from your
     branch), check that the files it names are yours, and check that you're
     back on your branch before committing: on
     zerobull-consulting/dagger#182 it reviewed an earlier PR's commits and
     left `HEAD` detached.
   - Then mark the PR ready. That starts **review 1**.
3. **The fix round:**
   - Trace every finding: red, yellow, blue, and any you can read only in its
     session.
   - Fix the real ones, each with a test that fails without the fix.
   - Answer the others on their thread with what you traced, and resolve the
     threads you've handled.
   - **Then run `/code-review high` again** (same range and checks as in
     step 2) on the fixed branch, and fix its real findings too.
   - Push all of it in **one push**. Gather everything into that push: no
     README touch-ups, rebases or follow-up commits in between.
   - *Why the second self-review:* review 2 is the last look before the
     merge, and nothing can be pushed after it, so a mistake in the fixes
     themselves is otherwise only filed, not fixed. In the round of
     zerobull-consulting/dagger#180 to #190, it caught real problems in the
     fix pushes of zerobull-consulting/dagger#186 (a duplicated deadline, a
     ref check's scope) and #188 (an unfiltered publish-run query), and no
     PR needed a third Copilot review.
   - **Waiting on Copilot or CI:** a review can land without waking the
     session. Never end a turn while waiting on Copilot or CI without first
     scheduling a check-in to yourself about 10 minutes out (the Claude Code
     Remote `send_later` tool, where you have it), and then read the PR's
     reviews and checks yourself. Three sessions in that round went idle
     beside a review that had already arrived (zerobull-consulting/dagger#180,
     #182, #188) and sat until someone nudged them.
4. **After review 2, push nothing more.** Handle whatever it reports (new
   findings, a 🔵 "Needs a closer look" with or without text, findings only in
   its session) without changing the PR:
   - **Confirmed** (you traced a real path to the failure): file an issue that
     references the tracking issue, says how to reproduce it, and asks for a
     known-gap test or the fix. The test comes with that later work, not in
     this PR.
   - **Not confirmed, or text you can't read:** append them to the tracking
     issue's **rolling leads issue** (file, lines, type, and the PR they came
     from), with no test. Find it by title ("unverified leads", under the
     tracking issue); open one only if none exists. Don't open a leads issue
     per PR: twelve of those piled up untriaged.
     Don't ask for Copilot's session findings.
   - Answer each thread with its issue link, and resolve it.
   - **The one exception:** a confirmed finding that means the PR deletes
     data it shouldn't, publishes an unverified blob or bundle, or exposes a
     token is fixed with a test in one more push, instead of being filed. The
     review that push starts is handled like review 2: nothing more is pushed
     for it. If the fix needs a design decision, don't merge: stop and report.
5. **Merge:** squash-merge review 2's head, or the head of a push step 4's
   exception or step 6's CI fix allowed after it, once the ruleset's required
   checks have passed on that head, with the title and message set as in
   section 4, pinned to that head commit. Where the auto-merge tool can set
   the title and message and pin the head, enabling auto-merge (squash) is
   fine. Who does it:
   - **By default, the session.**
   - **When the prompt says the user merges,** the session stops at this
     step and reports.
   - **When an orchestrating session started it** (a session the requester
     told directly to run the items and merge them), the session stops here
     and sends the orchestrator a **"ready to merge" report** (the Claude
     Code Remote `send_message` tool, `session_id` `@parent`): the head SHA,
     the Copilot reviews used, what the self-reviews and Copilot found and
     what was fixed, issues and leads filed, and the exact squash title and
     message. The orchestrator then checks it itself, before merging: the
     ruleset's required checks passed on that head, no more than two Copilot
     reviews and nothing pushed after review 2, and the prompt verbatim in
     the PR. Then it merges as above, does section 13's after-merge steps,
     and tells the session.
   - *Why the orchestrator merges:* a session reads an approval quoted in a
     prompt that another session wrote as an automated message, not the
     requester's own, and won't merge on it. That's the right call, and
     telling it again from the orchestrator carries the same weight. The #170
     session stopped short of merging (zerobull-consulting/dagger#176 was then
     merged with GitHub's default `(#176)` title), and the #87 session
     asked for confirmation before its reviews. Splitting the work this way let
     six PRs in a row (zerobull-consulting/dagger#182 to #190) merge without
     the requester.
6. **CI red at any point:** fix it at the root cause; never retry or loosen a
   check. A fix push after review 2 is allowed only for CI that is red on
   review 2's head, and its review is handled like review 2's.
7. **After the merge:**
   - Close the PR's issue by hand (the orchestrator does this when it
     merged), and unsubscribe.
   - Report how many review rounds were used, what was fixed, and which
     issues were filed.

**Stop instead of merging** only when CI stays red after a genuine fix, or for
the exception in step 4. Copilot's verdict alone doesn't block a merge once
review 2 has come back.

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
- **Decisions that are the requester's are settled before the session starts,**
  and written into the prompt as decided: credentials, engine changes,
  ruleset changes, design forks. A question that only the requester can answer
  stalls a session that hits it.
- **Who merges** is stated: by default the session, as in section 12. A
  prompt written by an orchestrating session says the orchestrator merges,
  and asks for the "ready to merge" report of section 12, step 5.
- When picking several items to run in parallel: refresh the state first
  (commits, open PRs, issues filed since the last round, and running sessions,
  not just PRs); map which files each item touches and avoid two items in the
  same hot file; follow the tracking issues' stated order; pair a critical-path
  item with an independent one and, where there is one, a small reliability
  fix such as a CI flake, which speeds up the others.
- When running items **one at a time** instead, refresh `feature/remote-cache`
  and the **Version** line before each launch, and carry what the last
  session hit into the next prompt's section 11.
- End the prompt with a line naming the template version it was generated
  from:

  > Generated from `hack/remote-cache/PROMPT_TEMPLATE.md` as of
  > `zerobull-consulting/dagger#<N>`

  where `<N>` is the PR in the file's **Version** line, at the top, as the file
  is on `feature/remote-cache`.

  The version is a PR, not a commit: before the branch's own review into
  `main`, `feature/remote-cache` will be flattened into new commits, so its
  commit hashes don't last. A PR number does, and the PR says why the process
  changed.

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
- The PR says what changed in the process and why, and sets the **Version**
  line to its own number.
- The next prompt generated names that PR (see the last convention above),
  so every prompt shows which version of the process it followed.
- The draft in zerobull-consulting/dagger#95's body and the working
  agreement's comments on zerobull-consulting/dagger#1 are history; this file
  supersedes them.
