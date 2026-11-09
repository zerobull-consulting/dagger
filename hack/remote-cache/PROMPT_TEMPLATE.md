# Remote cache prompt template

**Version:** [zerobull-consulting/dagger#194](https://github.com/zerobull-consulting/dagger/pull/194)

Every work item's prompt for the remote cache work is generated from this
file, by the session's lead (see [The lead](#the-lead)). It holds the
template, the steps every session follows (section 12, copied verbatim into
every prompt), the reasons behind them, and the conventions for the whole
prompt. It is the process's source, and it changes by PR (see [Changing this
file](#changing-this-file)).

**How the pieces fit:**

- **Issues stay general:** what and why, readable by a human or an agent
  (like [zerobull-consulting/dagger#63](https://github.com/zerobull-consulting/dagger/issues/63),
  [zerobull-consulting/dagger#65](https://github.com/zerobull-consulting/dagger/issues/65),
  [zerobull-consulting/dagger#94](https://github.com/zerobull-consulting/dagger/issues/94)).
- **The full prompt goes in the PR it started,** verbatim, in a collapsed
  block. It's the record of how that execution was instructed.
- **The template is how prompts are generated,** and where the process
  improves.

## The lead

**The lead is whoever starts a session:** a human, usually generating the
prompt from this file with an AI rather than typing it, or an orchestrator
session the human told directly to run the work. Every session has exactly
one lead, and the steps call it "your lead".

The lead writes two things for each session:

1. **The standing authorization:** who the lead is, what the session may do
   without asking, what it may not, and how it tells the lead when it stops.
   It goes in the session's system prompt (below).
2. **The prompt:** what to build and how (the template, next).

The lead then answers the session's questions, decides what to do with
findings the session can't confirm, checks the "ready to merge" report, and,
when the lead merges, merges and closes the issue.

### The standing authorization

An orchestrator session puts this in the new session's system prompt (the
`append_system_prompt` field of the Claude Code Remote `create_session`
tool), not in the prompt: a session gives a prompt another session wrote the
weight of an automated message, and won't open a PR or merge on it. A human
starting a session by hand puts it at the top of the first message, since
their own words already carry that weight.

Fill it in like this:

- **Your lead:** keep one of the two lines. For an orchestrator, replace
  SESSION_ID with its session ID and WHAT_THEY_ASKED with what the requester
  asked for, in their words.
- **Who merges:** when the session merges, keep the lines marked "(session
  merges)" and delete the one marked "(lead merges)". When the lead merges,
  do the reverse.
- **Branch:** replace BRANCH with the branch name from the prompt's "Branch
  and PR".

````markdown
## Standing authorization for this session

Your lead is:
- the requester, a human.
- the orchestrator session SESSION_ID, which the requester told directly to
  run this work. In their own words, the requester asked for WHAT_THEY_ASKED.

Within the task in the first message, you may, without asking first:
- create, commit to and push your own branch `BRANCH` in
  zerobull-consulting/dagger (never force-push, and never push to any other
  branch);
- open your PR as a draft against `feature/remote-cache`, edit its
  description, and mark it ready for review;
- reply to and resolve review threads on your own PR, comment on it, and
  re-request a Copilot review once for each run that failed;
- file issues in zerobull-consulting/dagger, as the steps describe;
- merge your own PR, as the steps describe (session merges);
- close the issue your PR closes, once it has merged (session merges).

You may not, without asking first:
- merge any PR or close any issue (lead merges);
- change repository settings or rulesets.

Your lead does those.

**Telling your lead when you stop:** when you stop on a question or a
blocker, tell your lead before you end the turn. An orchestrator gets it by
the Claude Code Remote `send_message` tool (`session_id` `@parent`); a human,
in your reply. A routine wait on CI or Copilot, with a check-in scheduled,
needs no message. Never end a turn waiting on a human without telling your
lead.
````

## The template

A prompt has two parts. Sections 1 to 11 are written for each item. Section 12
is the same in every prompt: copy it verbatim, from its first line to its
last, and don't summarize it or give it by reference. Keep the sections in
this order, and leave out a section only when it would be empty.

An example comes first, then each section in detail.

### Example

An illustration, not a real item: `<issue>`, `<other>` and `<N>` stand in
for real numbers. Everything else is the shape a real prompt takes, down to
how much each section says.

````markdown
# `lifecycle claim`: a `-dry-run` flag (zerobull-consulting/dagger#<issue>)

## 1. Header
Repository: zerobull-consulting/dagger. You're opening a PR that closes
zerobull-consulting/dagger#<issue>: `cacheservice lifecycle claim` writes a
bucket's lifecycle rules with no way to preview them. Why now: `claim` is
about to run against the shared Spaces bucket, and whoever runs it wants to
see the rules before they land.

## 2. Before starting, read
- zerobull-consulting/dagger#<issue>, and its first comment (the rule
  format the operator expects to see).
- `cmd/cacheservice/lifecycle/command.go` (how `claim` parses its flags and
  applies the rules) and `rules.go` (how the rules are built).
- zerobull-consulting/dagger#177, which added `claim`, and
  zerobull-consulting/dagger#180, which settled `-max-rules`: don't redo
  either.

## 3. Prerequisites
None.

## 4. Branch and PR
- Branch `claude/lifecycle-claim-dry-run-<issue>` from
  `origin/feature/remote-cache`.
- Closing keywords don't fire on feature branches; the issue is closed by
  hand (section 10).
- Who merges: your lead, the orchestrator session that started you.

## 5. Design
- **Decided:** a dry run makes no write call to the bucket, of any kind.
- **Proposed, confirm or change in the PR:** print the rules as the same
  JSON `claim` would send, and exit 0.

## 6. What to build
1. A `-dry-run` flag on `claim`.
2. With it, `claim` prints the rules and skips the write.
3. The flag in the README's `lifecycle claim` section.

## 7. Out of scope
A dry run for the other `lifecycle` subcommands
(zerobull-consulting/dagger#<other>).

## 8. Tests
- Unit: a fake S3 client that fails the test on any write call.
  `go test -race -count=1 ./cmd/cacheservice/...`
- MinIO: one run of `claim -dry-run` against the compose MinIO
  (`hack/remote-cache/compose.yaml`), showing the bucket's rules unchanged.

## 9. Done when
- `claim -dry-run` prints the rules `claim` would write, and the MinIO run
  shows the bucket's rules unchanged.
- A unit test fails if a dry run makes any write call.

## 10. After the merge
Your lead closes zerobull-consulting/dagger#<issue> with a comment linking
the merged PR.

## 11. Heads up
- Environment: use the compose MinIO; never point a test at the shared
  Spaces bucket.
- Lessons: none yet for `lifecycle`.

## 12. The rules and the steps
(section 12 of `hack/remote-cache/PROMPT_TEMPLATE.md`, copied verbatim)

Generated from `hack/remote-cache/PROMPT_TEMPLATE.md` as of
`zerobull-consulting/dagger#<N>`
````

### 1. Header

Repository; "You're opening a PR that closes <full reference>", with the
issue in a sentence; where it sits in the plan; and *why it matters now*.

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

- Branch name and base.
- The closing-keyword caveat: keywords don't fire on feature branches, so
  close by hand.
- **Who merges:** the session (the default), or its lead. It must match the
  standing authorization. Section 12's merge step covers both.

The rest (draft first, full references, the prompt in the PR, the squash title
and message) is in section 12, so it isn't repeated here.

*Why:* repository quirks that bit earlier rounds, and who has the last word.

### 5. Design

Each point marked **decided** (settled by the requester or the lead before
launch) or **proposed, confirm or change in the PR** (the session's to
judge). Section 12 has the session write its design choices into the PR
description.

*Why:* separates what was settled before launch from what the session
decides, and puts the reasoning on the record before code.

### 6. What to build

A numbered list of concrete changes.

*Why:* a checklist the PR can be checked against.

### 7. Out of scope

What's excluded, and which issue owns it.

*Why:* stops scope creep, and keeps neighbouring issues coherent.

### 8. Tests

By layer (a fake Dagger engine, MinIO, the built binary, real Dagger
engines), with the exact commands that must pass.

*Why:* every layer has caught bugs the others missed.

### 9. Done when

Observable outcomes, each shown by a test or a run.

*Why:* an end the agent can check for itself.

### 10. After the merge

Issues to close by hand, comments to post and where, and follow-up issues to
file. Say who does each (the lead, when it merges).

*Why:* keeps the issues that depend on this one current without the
requester doing it.

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

Copy everything from here to the end of the last step into the prompt,
verbatim.

<!-- BEGIN VERBATIM: copy from here -->

**The rules. These are plain on purpose: no jokes, no exceptions beyond the
ones written here.**

- **See it run before you write it down.** How the Dagger engine (the
  upstream code the cache service sits beside) or the S3 bucket (MinIO in
  tests, DigitalOcean Spaces for real) behaves goes into an issue, a doc or
  a code comment only after a real run shows it. Reading the code isn't
  enough.
- **The Dagger engine never makes the service delete.** A Dagger engine
  event such as `engine.prune` or a cache wipe can make the service forget
  entries, but objects leave the bucket only through GC.
- Bound every loop, retry, wait and buffer.
- Validate any string that came from the Dagger engine or the bucket before
  it goes into a metric label, an object key or a file name.
- Stage files explicitly. Never commit built binaries or `dagger.lock`
  changes by accident.
- Full `owner/repo#N` references everywhere: a bare `#N` can link to the
  upstream repository.
- **Never stop silently.** When you stop on a question or a blocker, tell
  your lead before you end the turn. A routine wait on CI or Copilot, with a
  check-in scheduled, needs no message.

**The steps. One at a time, top to bottom. Each one is one thing.** "Your
lead" is whoever started you, named in your standing authorization: a human,
or an orchestrator session. You reach an orchestrator by the Claude Code
Remote `send_message` tool (`session_id` `@parent`), and a human in your
reply.

*Get set up*

1. **Read.** Everything in "Before starting, read", and the whole prompt. All
   of it. Coffee's on us. A prerequisite missing, or the prompt unclear? Stop
   and ask your lead before you build anything.
2. **Record:** open a scratchpad file outside the repo (never committed).
   You *will* forget some of this if you don't write it down.

*Build it*

3. **Build.** Now, after all that reading: write the code. Do your best job to
   keep the code simple and the tests ample. Hit a design decision the prompt
   didn't make, or need a change to the Dagger engine or something you can't
   undo? Stop and ask your lead. (The `fork paths` check fails any change
   outside the service's paths.)
4. **Test.** Run the prompt's test commands until they pass. Then prove each
   new test can fail: break what it guards, watch it go red, put it back.
   They could call you the King's Men, 'cause you can break and fix Humpty
   all day.
5. **Commit** and **measure.** Commit, then run:

   ```bash
   git fetch origin feature/remote-cache &&
     git diff --numstat origin/feature/remote-cache...HEAD |
     awk '{n += $1 + $2} END {print n}'
   ```

   Over about 2,500 lines: stop and split it into issues, now, before
   anybody reviews it. Ask your lead for help. This number decides it once,
   and nothing later reopens it.

*Self-review, round 1: before anyone else sees it*

6. **Review:** run exactly
   `/code-review high origin/feature/remote-cache...HEAD`. You need somebody
   who can call you out when you're trippin' in order to be your best self.
   Then check that the files it names are yours, and that you're still on
   your branch (`git status`): it has wandered off to other commits before.

   *Go home early (a little):* found nothing? Skip the next two steps.

7. **Reject** findings that are wack: ones the test suite you can see with
   your own two eyes contradicts. Write each one and your reasoning into the
   scratchpad. A finding that needs a significant design change? Don't
   reject it and don't fix it: ask your lead, and wait for their answer
   before you go on.
8. **Fix** each finding you kept, and run the tests. You got this. Don't call
   it done until the fix is slappin' and the tests are passin'. **Commit**
   what you fixed.

*Bail early:* nothing committed in round 1, because it found nothing or you
rejected it all? Round 2 would just review the same code again. Skip to "The
draft".

*Self-review, round 2: check the fixes*

9. **Review again,** same exact command. It reads commits, not your working
   tree, which is why you committed. Fresh eyes on fresh code. Check the
   files and your branch again.

   *Go home early (a little more):* found nothing? Skip the next two steps.

10. **Reject** what doesn't hold up. Same bar as before, new scratchpad
    entries, and a finding that needs a design change still goes to your
    lead.
11. **Fix** what does, test it, **commit.** Look at you go.

*The draft: CI starts here*

12. **Open the draft.** Push, and open the PR as a **draft** against
    `feature/remote-cache`. Copilot doesn't review drafts, but every push
    runs CI, so push when you have something to show. Follow the
    repository's PR template, if it has one. The description says:
    - what the PR does;
    - the size you measured (and why, if it's over 2,500 and not split);
    - **the design choices you made:** what you weighed, what you picked and
      why, and where you'd want a reviewer to push back. Give this real
      thought: it's the part nobody can get from the diff;
    - at the very end, the prompt, verbatim (not your standing
      authorization):

    ```markdown
    ## Prompt

    <details><summary>The prompt that started this PR</summary>

    (the prompt, verbatim)

    </details>
    ```

    Keep the blank lines around the prompt, so its Markdown renders.
13. **Check in later.** Before you end any turn while waiting on CI or
    Copilot, schedule a check-in to yourself about 10 minutes out (the Claude
    Code Remote `send_later` tool). Never go idle without one. When it fires,
    read the PR's checks and reviews yourself: they can land without waking
    you. This goes for every wait from here on.
14. **Get CI green.** Red? Fix it. Test it. Repeat until green. Draft pushes
    are cheap insurance. Fix the root cause: never retry, skip, loosen or
    quarantine a check.

*Copilot, round 1*

15. **Mark "ready for review",** now that CI is green. That starts Copilot
    review 1. Check in later. If the review fails to run, re-request it once.
    If the re-run fails too, tell your lead and wait for their answer: no
    review isn't a clean review. Never push to trigger a review.
16. **Record** every finding from review 1 in the scratchpad: red, yellow,
    blue, and anything you can only read in its session.

    *Go home early:* review 1 came back clean and CI is green? You're done
    reviewing. Skip to "Wrap it up". Nice.

17. **Reject** the ones that don't hold up, same bar as your self-reviews.
    Reply on each thread with what you traced, and resolve it. Scratchpad it.
    One that needs a design change goes to your lead; reply on its thread
    once they've answered.
18. **Fix** the real ones, each with a test that fails without the fix. If CI
    went red on the ready PR, fix that too, at the root cause, same as on the
    draft. Run the tests, then read your whole diff once more yourself: nobody
    else checks these fixes before Copilot does. **Commit. Do not push yet.**
    Hold that push like it's the last slice of pizza.

    *Go home early:* nothing to push because you rejected everything? Don't
    push. Skip to "Wrap it up".

19. **Push, once.** One push carrying every commit since you marked it ready.
    It's your one fix push, and it starts Copilot review 2. No touch-ups or
    rebases sneaking in alongside. Check in later. Review 2 fails to run? Same
    deal: re-request it once, then tell your lead and wait for their answer.

*Copilot, round 2: hands off the code*

20. **Record** every finding from review 2: new findings, a 🔵 "Needs a
    closer look" with or without text, findings only in its session. From
    here on you don't push, except for the two cases below. You've done the
    work. Let it ride.
21. **File** each *confirmed* finding (you traced a real path to the failure)
    as an issue that references the issue your PR closes, says how to
    reproduce it, and asks for a known-gap test or the fix. The test comes
    with that later work, not this PR. Reply on its thread with the issue
    link, and resolve it.
22. **Answer** each finding you traced and found wrong: reply on its thread
    with what you traced, resolve it, and scratchpad it.
23. **Ask.** If Copilot gives a "hedgy", unreadable, or untraceable finding,
    ask your lead what to do with that finding. Do what they say, then reply
    on its thread and resolve it.
24. **The only two pushes allowed after review 2:**
    - CI is red on review 2's head: fix the root cause. No retries, skips or
      loosened checks here either.
    - A confirmed finding means the PR deletes data it shouldn't, publishes
      an unverified blob or bundle, or exposes a token: fix it, with a test.

    Either push starts a review that's handled like review 2: nothing more is
    pushed for it.

*Wrap it up*

25. **File little issues.** Your value here is your thought. Go through the
    scratchpad: every rejection that deserves a follow-up gets its **own tiny
    issue** referencing the issue your PR closes. One idea per issue. Small
    radius, every time. Findings you already filed after review 2 are done,
    and rejections that were plainly wrong (the tests contradict them) need
    no issue.
26. **Merge, or hand off.** Check the prompt's "Done when" list first.
    **Stop and report instead of merging** if an item on it isn't met, if CI
    stays red after a genuine fix, or if a fix after review 2 needs a design
    decision. Copilot's verdict alone never blocks the merge. The merge is a
    squash of the final head, once `feature/remote-cache`'s ruleset's
    required checks (read them from the ruleset, not the PR's checks list)
    have passed on it, pinned to that head, with the title and message set
    explicitly: the title is the PR's title plus
    `(zerobull-consulting/dagger#<PR>)`, never GitHub's default `(#<PR>)`; the
    message is the PR's summary, without the prompt. Then, depending on who
    merges (the prompt's "Branch and PR" says):
    - **You** (the default): merge it.
    - **Your lead:** send them a **"ready to merge" report**: the head SHA,
      Copilot reviews used, what the self-reviews and Copilot found, what you
      fixed, the issues you filed, and the exact squash title and message. A
      session gets it by the Claude Code Remote `send_message` tool
      (`session_id` `@parent`); a human, in your reply. They check it, merge,
      and tell you.
27. **After the merge:** do your part of the prompt's "After the merge" list
    (your lead does the rest if they merged), and unsubscribe from the
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

**Why two self-reviews, both before the draft.** A second self-review checks
the fixes from the first. In the round of zerobull-consulting/dagger#180 to
zerobull-consulting/dagger#190, a second self-review caught real problems in
fixes on zerobull-consulting/dagger#186 (a duplicated deadline, a ref check's
scope) and zerobull-consulting/dagger#188 (an unfiltered publish-run query),
and no PR needed a third Copilot review. Back then it ran between the Copilot
rounds, to check the fixes for review 1. Interleaving it there made the flow a
loop sessions got lost in, so both self-reviews now run up front, and the fixes
for Copilot review 1 get the tests and a read of the whole diff instead. The
trade: a mistake in those fixes that only `/code-review` would catch is now
filed after review 2, not fixed. Both reviews run before the draft so CI only
spends on code that's been reviewed. Fixes are committed before the next
review because the range compares commits only
(zerobull-consulting/dagger#193).

**Why the check-ins.** A review can land without waking the session. Three
sessions in that round went idle beside a review that had already arrived
(zerobull-consulting/dagger#180, zerobull-consulting/dagger#182,
zerobull-consulting/dagger#188) and sat until someone nudged them.

**Why the session asks its lead about unverified findings.** One leads issue
per PR meant twelve of them piled up untriaged. One rolling leads issue fixed
that, but it was hard to explain to a session working on a single task.
Deciding what a lead is worth, and what it belongs with, takes a view across
tasks, which the lead has and the session doesn't.

**Why the standing authorization goes in the system prompt.** A session reads
an approval quoted in a prompt that another session wrote as an automated
message, not the requester's own, and won't act on it. That's the right
call, and telling it again from the orchestrator carries the same weight. So
the orchestrator merged instead, and the authorization moved out of the
prompt and into the session's system prompt, where it arrives as setup. The
zerobull-consulting/dagger#184 session pushed its fix and then stopped before
opening its PR, asking for a go-ahead its prompt already gave. The question
went to its own status line, where only the requester would see it, and
nothing reached the orchestrator until a fallback check. Hence the rule to
tell your lead when you stop on a question or a blocker. The
zerobull-consulting/dagger#170 session stopped short of merging
(zerobull-consulting/dagger#176 was then merged with GitHub's default
`(#176)` title), and the zerobull-consulting/dagger#87 session asked for
confirmation before its reviews. Having the orchestrator merge let six PRs
in a row (zerobull-consulting/dagger#182 to zerobull-consulting/dagger#190)
merge without the requester.

**Why "see it run before you write it down".** zerobull-consulting/dagger#127
corrected a code-reading claim about the Dagger engine that a real kept-volume
run disproved. The rule used to name "GitHub's" behaviour too, with no
incident behind it, so it was dropped.

**Why one linear list, copied verbatim, in an informal voice.** The steps used
to be split across sections 9 to 14, given by reference, with nested "why"
bullets and loops ("fix, then review again, then gather it into one push").
Sessions got lost in the middle, and zerobull-consulting/dagger#193 was one
result. One step per action, written out again instead of "repeat", with
different wording each time, makes the second round visibly not the first.
The rules stay plain so the limits are never ambiguous. The "go home early"
exits and the tiny follow-up issues reward a PR that's going well and keep
each change small (zerobull-consulting/dagger#198).

## Conventions for the whole prompt

- Say when to **stop and ask** rather than decide: design decisions, a missing
  prerequisite, anything irreversible.
- **Measure before choosing a number,** and put the measurement in the PR.
- **Decisions that are the requester's are settled before the session starts,**
  and written into the prompt as decided: credentials, Dagger engine changes,
  ruleset changes, design forks. A question that only the requester can answer
  stalls a session that hits it.
- **Who merges** is stated in section 4 and in the standing authorization,
  and the two agree: by default the session. When the lead merges, the merge
  step becomes a "ready to merge" report.
- **Before the lead merges,** it checks the report against the PR: the
  ruleset's required checks passed on the head being merged; at most two
  Copilot reviews, plus one for each push the steps allow after review 2 (a
  CI fix, or a fix for deleted data, an unverified blob or bundle, or an
  exposed token), and nothing else pushed after review 2; and the prompt
  verbatim in the PR. The steps don't state the cap as a number, so this is
  where it's checked.
- **When a session asks about a finding it couldn't confirm,** the lead
  decides: file it on its own, add it to an issue with similar ones, or drop
  it. The session never makes that call.
- **Run items one at a time,** in the plan's order. Before
  each launch, refresh `feature/remote-cache` and the **Version** line, and
  carry what the last session hit into the next prompt's "Heads up"
  (section 11). Running items in parallel turned out to be hard.
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

An orchestrator creates the session with the standing authorization as its
appended system prompt and the full prompt, section 12 included, as its
first message. A human pastes both into the first message, authorization
first. Either way, the lead then checks in on the session about 10 minutes
after launch, and every 20 minutes while it works.

The session copies the whole prompt, verbatim, into the draft PR it opens,
but never the standing authorization, even when a human pasted both into the
first message. A follow-up PR from the same prompt links to the original's
instead of copying it.

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
