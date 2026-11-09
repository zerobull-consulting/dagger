# Remote cache prompt template

**Version:** [zerobull-consulting/dagger#194](https://github.com/zerobull-consulting/dagger/pull/194)

Every work item's prompt for the remote cache work is generated from this
file. It holds the template, the steps every session follows (section 12,
copied verbatim into every prompt), the reasons behind them, and the
conventions for the whole prompt. It is the process's source, and it changes
by PR (see [Changing this file](#changing-this-file)).

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

Keep the sections in this order, and leave out a section only when it would
be empty. Sections 1 to 11 are written for each item. Section 12 is the same
in every prompt: copy it verbatim, from its first line to its last, and don't
summarize it or give it by reference.

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

- Branch name and base.
- The closing-keyword caveat: keywords don't fire on feature branches, so
  close by hand.
- **Who merges:** the session (the default), the user, or the orchestrating
  session. Step 27 of section 12 covers each case.

The rest (draft first, full references, the prompt in the PR, the squash title
and message) is in section 12, so it isn't repeated here.

*Why:* repository quirks that bit earlier rounds, and who has the last word.

### 5. Design

Each point marked **decided** (the requester's) or **proposed, confirm or
change in the PR** (the agent's to judge). Section 12 has the agent write its
proposals down before building on them.

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
commands that must pass.

*Why:* every layer has caught bugs the others missed.

### 9. Done when

Observable outcomes, each shown by a test or a run.

*Why:* an end the agent can check for itself.

### 10. After the merge

Issues to close by hand, comments to post and where, and follow-up issues to
file. Say who does each (the orchestrator, when it merges).

*Why:* keeps the tracking issue and the issues that depend on this one current
without the requester doing it.

### 11. Heads up

A short list, kept to what this item will actually hit:

- **Environment:** the sandbox's quirks it will meet (the Docker Hub mirror,
  the TLS CA bundle, the unsampled `TRACEPARENT`, which buckets not to touch).
- **Lessons:** what earlier sessions hit that applies here. This is where
  process improvements go first, before they become steps or rules.
- **Extra rules:** anything beyond section 12's that only this item needs.

*Why:* each environment quirk cost a round once, and lessons belong next to
the work they're about.

### 12. The rules and the steps

Copy everything from here to the end of step 28 into the prompt, verbatim.

<!-- BEGIN VERBATIM: copy from here -->

**The rules. These are plain on purpose: no jokes, no exceptions beyond the
ones written here.**

- At most **two** Copilot reviews. While the PR is a draft, push as often as
  you like. Once it's ready, there is exactly **one** push (step 20), and
  **nothing** after Copilot review 2 except the two cases in step 25.
- CI red: fix the root cause. Never retry, skip, loosen or quarantine a check.
- No engine changes without stopping to ask first. The `fork paths` check
  fails any change outside the service's paths.
- **Verify by running:** a claim about the engine's, GitHub's or a store's
  behaviour goes into an issue, a doc or the code only after a real run shows
  it, not from reading code.
- Never delete on an engine's signal. Bound everything. Validate any engine-
  or bucket-supplied string before it goes into a label, key or file name.
- Stage files explicitly. Never commit built binaries or `dagger.lock`
  changes by accident.
- Full `owner/repo#N` references everywhere: a bare `#N` can link to the
  upstream repository.
- Stop and ask, instead of deciding, on a design decision that's the
  requester's, a missing prerequisite, or anything irreversible.

**The steps. One at a time, top to bottom. Each one is one thing.**

*Get set up*

1. **Read.** Everything in "Before starting, read", and the whole prompt. All
   of it. Coffee's on us.
2. **Record:** open a scratchpad file outside the repo (never committed) for
   findings, decisions, rejections and anything weird. You *will* forget some
   of this if you don't write it down.
3. **Propose.** Write each "proposed" design point down in the scratchpad,
   with your call and why. It goes into the PR description in step 7, before
   anyone reviews code built on it.

*Build it*

4. **Build.** Now, after all that reading: write the code. Do your best job
   to keep the code simple and the tests ample.
5. **Test.** Run the prompt's test commands until they pass. Then prove each
   new test can fail: break what it guards, watch it go red, put it back. A
   test that can't fail is just vibes.
6. **Commit** and **measure.** Commit, then run this and write the number in
   the scratchpad:

   ```bash
   git fetch origin feature/remote-cache &&
     git diff --numstat origin/feature/remote-cache...HEAD |
     awk '{n += $1 + $2} END {print n}'
   ```

   Over about 2,500 lines: stop and split it into smaller PRs, now, before
   anyone reviews it. You decide this once, from this number, and never
   again.

*The draft*

7. **Open the draft.** Push, and open the PR as a **draft** against
   `feature/remote-cache`. Copilot doesn't review drafts, so draft pushes are
   free. Follow the repository's PR template, if it has one. The
   description says what the PR does, the step 6 size (and why, if
   it's over 2,500 and not split), your design calls from step 3, and, at the
   very end, the prompt verbatim:

   ```markdown
   ## Prompt

   <details><summary>The prompt that started this PR</summary>

   (the prompt, verbatim)

   </details>
   ```

   Keep the blank lines around the prompt, so its Markdown renders.
8. **Check in later.** Before you end any turn while waiting on CI or
   Copilot, schedule a check-in to yourself about 10 minutes out (the Claude
   Code Remote `send_later` tool). Never go idle without one. When it fires,
   read the PR's checks and reviews yourself: they can land without waking
   you. This goes for every wait below.

*Self-review 1: before anyone else sees it*

9. **Review:** run exactly
   `/code-review high origin/feature/remote-cache...HEAD`. You need somebody
   who can call you out when you're trippin' in order to be your best self.
   Then check that the files it names are yours, and that you're still on your
   branch (`git status`): it has wandered off to other commits before.
10. **Reject** findings that are wack: ones that need a significant design
    change, or that the test suite you can see with your own two eyes
    contradicts. Write each rejection and your reasoning into the scratchpad.
11. **Fix** each finding you accepted, and run the tests. You got this. Don't
    call it done until the fix is slappin' and the tests are passin'. Commit,
    and push to the draft.

    *Go home early (a little):* if step 9 found nothing, skip 10 and 11.

12. **Get CI green.** Red? Root cause, commit, push to the draft, check in
    later, look again. Repeat until green. Draft pushes are still free.

*Copilot review 1*

13. **Mark ready.** That starts Copilot review 1. Check in later.
14. **Record** every finding from review 1 in the scratchpad: red, yellow,
    blue, and anything you can only read in its session.
15. **Reject** the ones that don't hold up, same bar as step 10. Reply on
    each thread with what you traced, and resolve it. Write it down too.
16. **Fix** the real ones, each with a test that fails without the fix. Run
    the tests. If CI went red on the ready PR, fix that here too. **Commit
    locally. Do not push.** Hold that push like it's the last slice of pizza.

    *Go home early:* if CI is green and there's nothing committed since step
    13 (review 1 came back clean, or you rejected everything it said), there's
    nothing for review 2 to look at. Skip to step 26. Nice.

*Self-review 2: checks your fixes*

17. **Review:** run exactly
    `/code-review high origin/feature/remote-cache...HEAD` again. It reads
    commits, not your working tree, which is why step 16 committed. Fresh eyes
    on fresh code. Check the files and your branch again, like step 9.
18. **Reject** what doesn't hold up. Same bar, new scratchpad entries.
19. **Fix** what does, test it, **commit** locally. Almost there. Keep your
    head in the game.
20. **Push, once.** One push carrying every commit since step 13. This is
    your one fix push, and it starts Copilot review 2. No touch-ups or rebases
    sneaking in alongside. Check in later.

*Copilot review 2: hands off the code*

21. **Record** every finding from review 2: new findings, a 🔵 "Needs a
    closer look" with or without text, findings only in its session. From
    here on you don't push. You've done the work. Let it ride.
22. **File** each *confirmed* finding (you traced a real path to the failure)
    as an issue that references the tracking issue, says how to reproduce it,
    and asks for a known-gap test or the fix. The test comes with that later
    work, not this PR.
23. **Answer** each finding you traced and found wrong: reply on its thread
    with what you traced, resolve it, and note it in the scratchpad.
24. **Park** each finding you couldn't confirm, or can't read, in the
    tracking issue's rolling leads issue (find it by title, "unverified
    leads"; open one only if none exists): file, lines, type, and this PR.
    Don't ask Copilot for its session findings. Reply on each thread with the
    issue link, and resolve it.
25. **The only two pushes allowed after review 2:**
    - CI is red on review 2's head: fix the root cause.
    - A confirmed finding means the PR deletes data it shouldn't, publishes
      an unverified blob or bundle, or exposes a token: fix it, with a test.

    Either push starts a review that's handled like review 2: nothing more is
    pushed for it.

*Wrap it up*

26. **File little issues.** Your value here is your thought. Go through the
    scratchpad: every rejection, and every finding you decided not to address,
    that deserves a follow-up gets its **own tiny issue** referencing the
    tracking issue. One idea per issue. Small radius, every time. Rejections
    that were plainly wrong (the tests contradict them) need no issue.
27. **Merge, or hand off.** Check the prompt's "Done when" list first.
    **Stop and report instead of merging** if an item on it isn't met, if CI
    stays red after a genuine fix, or if a step 25 fix needs a design
    decision. Copilot's verdict alone never blocks the merge. The merge is a squash of the final head, once `feature/remote-cache`'s
    ruleset's required checks (read them from the ruleset, not the PR's
    checks list) have passed on it, pinned to that head, with the title and
    message set explicitly: the title is the PR's title plus
    `(zerobull-consulting/dagger#<PR>)`, never GitHub's default `(#<PR>)`; the
    message is the PR's summary, without the prompt. Then, depending on who
    merges (the prompt's "Branch and PR" says):
    - **You:** merge it.
    - **The user:** stop here and report.
    - **The orchestrating session:** send it a **"ready to merge" report**
      (the Claude Code Remote `send_message` tool, `session_id` `@parent`):
      the head SHA, Copilot reviews used, what the self-reviews and Copilot
      found, what you fixed, the issues and leads you filed, and the exact
      squash title and message. It checks and merges, and tells you.
28. **After the merge:** do your part of the prompt's "After the merge" list
    (the orchestrator does the rest when it merged), and unsubscribe from the
    PR. Report review rounds used, what was fixed, and which issues were
    filed. Go home. You earned it.

<!-- END VERBATIM: copy to here -->

## Why the steps look like this

This section isn't copied into prompts. It's the history behind section 12,
so changes to the steps can be weighed against what taught us each one.

**The working agreement.** It was first posted on zerobull-consulting/dagger#1
as [the agreement](https://github.com/zerobull-consulting/dagger/issues/1#issuecomment-6639444472)
and [its amendment](https://github.com/zerobull-consulting/dagger/issues/1#issuecomment-6651252824)
(four rounds for every PR); this file supersedes both. Large PRs
(zerobull-consulting/dagger#56: 16 Copilot reviews;
zerobull-consulting/dagger#71: 6) kept collecting 🔵 "Needs a closer look"
reviews and never reached a clean one. With draft-first, a self-review before
marking ready, and one push per round, reviews stopped multiplying by the
number of commits: zerobull-consulting/dagger#76 (321 lines) took 3 Copilot
reviews, zerobull-consulting/dagger#78 (1,543) took 2, and
zerobull-consulting/dagger#79 (1,238) took 5. The cap is what bounds the cost,
and splitting large work keeps the rounds productive.

**How this repository is set up.** An org ruleset has Copilot review every
push to a ready PR, and not drafts, so each push is a review round.
`feature/remote-cache`'s ruleset is the source of truth for required checks;
a PR's checks list also shows optional ones. Copilot is not a required check.

**Why every push counts, and why two.** The earlier text capped "rounds" but
required a final push after the last one, so a cap of four meant five reviews
(zerobull-consulting/dagger#121 and zerobull-consulting/dagger#141 had five).
Under that cap, most PRs from zerobull-consulting/dagger#107 to
zerobull-consulting/dagger#168 already finished in two, thanks to the
self-review before marking ready, and zerobull-consulting/dagger#168 met every
target under a cap of two.

**Why split once, from the first diff.** Splitting large work where it splits
naturally cuts findings more than anything else. Deciding once, before any
review, stops fixes during the rounds from reopening it; the review cap limits
growth after that. The size counts every file, tests and generated ones
included, from committed `HEAD` (a binary file counts as 0).

**Why the explicit review range and the branch check.** On
zerobull-consulting/dagger#182, `/code-review` reviewed an earlier PR's
commits and left `HEAD` detached.

**Why the second self-review, and why step 16 commits.** Review 2 is the last
look before the merge, and nothing can be pushed after it, so a mistake in the
fixes themselves is otherwise only filed, not fixed. In the round of
zerobull-consulting/dagger#180 to zerobull-consulting/dagger#190, it caught
real problems in the fix pushes of zerobull-consulting/dagger#186 (a
duplicated deadline, a ref check's scope) and zerobull-consulting/dagger#188
(an unfiltered publish-run query), and no PR needed a third Copilot review.
The range compares commits only, so uncommitted fixes would go unchecked
(zerobull-consulting/dagger#193).

**Why the check-ins.** A review can land without waking the session. Three
sessions in that round went idle beside a review that had already arrived
(zerobull-consulting/dagger#180, zerobull-consulting/dagger#182,
zerobull-consulting/dagger#188) and sat until someone nudged them.

**Why one rolling leads issue.** One leads issue per PR meant twelve of them
piled up untriaged.

**Why the orchestrator merges.** A session reads an approval quoted in a
prompt that another session wrote as an automated message, not the
requester's own, and won't merge on it. That's the right call, and telling it
again from the orchestrator carries the same weight. The
zerobull-consulting/dagger#170 session stopped short of merging
(zerobull-consulting/dagger#176 was then merged with GitHub's default
`(#176)` title), and the zerobull-consulting/dagger#87 session asked for
confirmation before its reviews. Splitting the work this way let six PRs in a
row (zerobull-consulting/dagger#182 to zerobull-consulting/dagger#190) merge
without the requester.

**Why "verify by running".** zerobull-consulting/dagger#127 corrected a
code-reading claim that a real kept-volume run disproved.

**Why one linear list, copied verbatim, in an informal voice.** The steps used
to be split across sections 9 to 14, given by reference, with nested "why"
bullets and loops ("fix, then review again, then gather it into one push").
Sessions got lost in the middle, and zerobull-consulting/dagger#193 was one
result. One step per action, written out again instead of "repeat", with
different wording each time, makes step 17 visibly not step 9. The rules stay
plain so the limits are never ambiguous. The "go home early" exits and the
tiny follow-up issues reward a PR that's going well and keep each change
small (zerobull-consulting/dagger#198).

## Conventions for the whole prompt

- Say when to **stop and ask** rather than decide: design decisions, a missing
  prerequisite, anything irreversible.
- **Measure before choosing a number,** and put the measurement in the PR.
- **Decisions that are the requester's are settled before the session starts,**
  and written into the prompt as decided: credentials, engine changes,
  ruleset changes, design forks. A question that only the requester can answer
  stalls a session that hits it.
- **Who merges** is stated in section 4: by default the session. A prompt
  written by an orchestrating session says the orchestrator merges, which
  makes step 27 a "ready to merge" report.
- When picking several items to run in parallel: refresh the state first
  (commits, open PRs, issues filed since the last round, and running sessions,
  not just PRs); map which files each item touches and avoid two items in the
  same hot file; follow the tracking issues' stated order; pair a critical-path
  item with an independent one and, where there is one, a small reliability
  fix such as a CI flake, which speeds up the others.
- When running items **one at a time** instead, refresh `feature/remote-cache`
  and the **Version** line before each launch, and carry what the last
  session hit into the next prompt's "Heads up" (section 11).
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

Paste the full prompt, section 12 included. The agent copies the whole prompt,
verbatim, into the PR it opens (step 7). A follow-up PR from the same prompt
links to the original's instead of copying it.

## Changing this file

- The template changes by PR, like any other file, into `feature/remote-cache`.
- The PR says what changed in the process and why, and sets the **Version**
  line to its own number.
- A change to section 12 changes every prompt from then on. Put the reason in
  "Why the steps look like this".
- The next prompt generated names that PR (see the last convention above),
  so every prompt shows which version of the process it followed.
- The draft in zerobull-consulting/dagger#95's body and the working
  agreement's comments on zerobull-consulting/dagger#1 are history; this file
  supersedes them.
