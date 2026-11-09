# Remote cache prompt template

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
- **The standing authorization goes with every prompt:** in the session's
  system prompt from an orchestrator, or at the top of the first message from
  a human (see [The lead](#the-lead)). It never goes in the PR.

## The lead

**The lead is whoever starts a session:** a human, usually generating the
prompt from this file with an AI rather than typing it, or an orchestrator
session the human told directly to run the work. Every session has exactly
one lead, and the steps call it "your lead".

The lead writes two things for each session:

1. **The standing authorization:** who the lead is, what the session may do
   without asking, what it may not, and how it tells the lead when it stops.
   It goes in the session's system prompt, or at the top of a human's first
   message (below).
2. **The prompt:** what to build and how (the template, next).

After launch, the lead does everything outside the code:

- answers the session's questions;
- decides on every finding the session hands over, from every self-review
  round and every Copilot review (below), and whether a self-review block
  gets another round;
- decides on scope at the check-in after the first green CI, and splits the
  work into issues if it's too big;
- checks the "ready to merge" report, files the issues it decided on, merges,
  and closes the issue.

The session never files, amends or closes an issue, and never merges.

### Deciding on a finding

The session sorts each finding (security or data loss, real, wrong, or can't
tell) and hands the list over with its read on each. For each one, take the
first of these that fits:

1. **Fix it:** the session changes the code or the tests in this PR.
2. **Clarify it:** the session updates the docs, or you update the
   conventions in this file.
3. **Pin it:** the session adds a known-gap test in this PR that shows today's
   behaviour (named the way that file names its tests, with "known gap" in the
   name: `TestKnownGap…` in Go), and you file an issue for the fix that names
   the test. A pinned gap can't drift unnoticed, and the fix flips the test.
4. **File it:** you file an issue, with no test.
5. **Reject it:** the session notes why, and replies on the thread if it
   came from Copilot.

Security or data loss findings are fixed in this PR, or the PR doesn't merge.
Never describe a token leak in a public issue.

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
  re-request a Copilot review once for each run that failed.

You may not file, edit or close an issue, merge a PR, or change repository
settings or rulesets. Your lead does those.

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

An illustration, not a real item: `<issue>` and `<other>` stand in for real
numbers. Everything else is the shape a real prompt takes, down to how much
each section says. A real launch also needs a standing authorization, filled
in from [The standing authorization](#the-standing-authorization); this
example's would name an orchestrator lead that merges.

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
- Closing keywords don't fire on feature branches; your lead closes the
  issue by hand (section 10).

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
````

### 1. Header

Repository; "You're opening a PR that closes <full reference>", with the
issue in a sentence; what it builds on or unblocks; and *why it matters now*.

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
  the lead closes the issue by hand.

The rest (draft first, full references, the prompt in the PR, the squash title
and message, the lead merging) is in section 12, so it isn't repeated here.

*Why:* repository quirks that bit earlier rounds.

### 5. Design

Each point marked **decided** (settled by the requester or the lead before
launch) or **proposed, confirm or change in the PR** (the session's to
judge). Section 12 has the session write its design choices into the PR
description.

*Why:* separates what was settled before launch from what the session
decides, and puts the reasoning on the record where reviewers will see it.

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

Issues the lead closes by hand, comments it posts and where, and follow-up
issues it files. All of it is the lead's; the session only reports.

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
  entries, but never makes it remove their objects from the bucket.
- Bound every loop, retry, wait and buffer.
- Validate any string that came from the Dagger engine or the bucket before
  it becomes any key, label or name: a map key, a metric label, an object
  key, a file name.
- Stage files explicitly. Never commit built binaries or `dagger.lock`
  changes by accident.
- Full `owner/repo#N` references everywhere: a bare `#N` can link to the
  upstream repository.
- **Never stop silently.** When you stop on a question or a blocker, tell
  your lead before you end the turn. A routine wait on CI or Copilot, with a
  check-in scheduled, needs no message.
- **Waiting on your lead:** schedule a check-in to yourself about 10 minutes
  out (the Claude Code Remote `send_later` tool) before you end the turn. No
  answer after two check-ins? Send the question once more. Silence is never
  a no, and never a yes.

**The steps. One at a time, top to bottom. Each one is one thing.** "Your
lead" is whoever started you, named in your standing authorization: a human,
or an orchestrator session. You reach an orchestrator by the Claude Code
Remote `send_message` tool (`session_id` `@parent`), and a human in your
reply. Your lead makes every call about findings, files every issue, and
merges; you write the code.

*Get set up*

1. **Read.** Everything in "Before starting, read", and the whole prompt. All
   of it. Coffee's on us. A prerequisite missing, or the prompt unclear? Stop
   and ask your lead before you build anything.
2. **Record:** open a scratchpad file outside the repo (never committed).
   You *will* forget some of this if you don't write it down.

*Build it*

3. **Build.** Now, after all that reading: write the code. Do your best job to
   keep the code simple and the tests ample. Hit a design decision the prompt
   didn't make, or need a change to the Dagger engine? Stop and ask your lead.
   (The `fork paths` check fails any change outside the service's paths.)
4. **Test.** Run the prompt's test commands until they pass. Then prove each
   new test can fail: break what it guards, watch it go red, put it back.
   They could call you the King's Men, 'cause you can break and fix Humpty
   all day.
5. **Commit.**

*Self-review, block 1: before anyone else sees it*

6. **Review:** run exactly
   `/code-review high origin/feature/remote-cache...HEAD`. You need somebody
   who can call you out when you're trippin' in order to be your best self.
   Then check that the files it names are yours, and that you're still on
   your branch (`git status`): it has wandered off to other commits before.
   Not on it? Check your branch out again before you commit.

   *Go home early:* it found nothing? Skip to "The draft".

7. **Sort** each finding in the scratchpad: **security or data loss** (it's
   real, and the code deletes data it shouldn't, publishes an unverified blob
   or bundle, or exposes a token), **real** (you can name the input, the code
   path and the wrong result), **wrong** (you followed the path it names, and
   it doesn't go wrong), or **can't tell**. A finding about how the Dagger
   engine or the bucket behaves is real or wrong only once a run shows it;
   until then, it's can't tell.
8. **Hand it to your lead,** the whole list in one message, security or data
   loss first, with your read on each. Wait for their answer.
9. **Do what they say,** finding by finding: fix the code or the tests, update
   a doc, or add a known-gap test that pins today's behaviour (name it the
   way that file names its tests, with "known gap" in the name). Run the
   tests until they pass, prove any new test can fail, and **commit** what
   you changed. You got this. Don't call it done until the fix is slappin'
   and the tests are passin'. If your lead says "another round", go back to
   **Review**. Otherwise, on to "The draft".

*The draft: CI starts here*

10. **Local green.** Run the prompt's test commands one more time. All of them
    pass.
11. **Open the draft.** Push, and open the PR as a **draft** against
    `feature/remote-cache`. Copilot doesn't review drafts, but every push
    runs CI, so push when you have something to show. Follow the
    repository's PR template, if it has one. The description says:
    - what the PR does;
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
12. **Check in later.** Before you end any turn while waiting on CI or
    Copilot, schedule a check-in to yourself about 10 minutes out (the Claude
    Code Remote `send_later` tool), and subscribe to the PR's activity if you
    have the `subscribe_pr_activity` tool. Never go idle without a check-in.
    When it fires, read the PR's checks and reviews yourself: they can land
    without waking you. This goes for every wait from here on.
13. **Get CI green.** Red? Fix it. Test it. Repeat until green. Draft pushes
    are cheap insurance. Fix the root cause: never retry, skip, loosen or
    quarantine a check.
14. **Check in with your lead** after the first green CI. Give them the size
    of the diff and how you're feeling about the scope, and wait for them to
    give the approval to keep going. If they split the work, do what they
    say about this PR's scope, then get CI green again before you go on. The
    size is the lines added plus deleted, every file counted:

    ```bash
    git fetch origin feature/remote-cache &&
      git diff --numstat origin/feature/remote-cache...HEAD |
      awk '{n += $1 + $2} END {print n}'
    ```

*Copilot, round 1*

15. **Mark "ready for review".** That starts Copilot review 1. Check in
    later. If the review fails to run, re-request it once. If the re-run
    fails too, tell your lead and wait for their answer: no review isn't a
    clean review. Never push to trigger a review.

    *Go home early:* Copilot came back clean and CI is green? You're done
    reviewing. Skip to "Wrap it up". Nice.

16. **Take Copilot's findings:** every finding from its review (red, yellow,
    blue, and anything you can only read in its session). Sort each one, same
    four reads as before, then hand the list to your lead and ask for a
    decision on each. Wait for their answer.
17. **Do what they say,** run the tests, prove any new test can fail, and
    **commit what you changed. Do not push yet.** If CI went red on the ready
    PR, fix that too, at the root cause. Hold that push like it's the last
    slice of pizza. Reply on each of Copilot's threads with what you did, and
    resolve it.

*Self-review, block 2: check those fixes*

18. **Review:** run exactly
    `/code-review high origin/feature/remote-cache...HEAD` again. Fresh eyes
    on fresh code. Check the files and your branch again.

    *Go home early:* it found nothing? Skip to "Push, once".

19. **Sort** each finding, same four reads.
20. **Hand it to your lead,** the whole list in one message, and wait for
    their answer.
21. **Do what they say,** run the tests, prove any new test can fail, and
    **commit what you changed.** Still no push. If your lead says "another
    round", go back to **Review**. Otherwise, on to "Push, once".
22. **Push, once.** Nothing committed since you marked it ready? Don't push:
    skip to "Wrap it up". Otherwise run the tests until they're green, then
    make one push carrying every commit since you marked it ready. It starts
    Copilot review 2. No touch-ups or rebases sneaking in alongside. Check in
    later. Review 2 fails to run? Same deal: re-request it once, then tell
    your lead and wait for their answer.

*Copilot, round 2: hands off the code*

23. **CI red on review 2's head?** Fix the root cause and push: no retries,
    skips or loosened checks. That's the one push you don't need to ask for.
    Otherwise, from here on you push only when your lead says to. Any
    Copilot review after review 2 is handled like review 2: sort, hand over,
    and no further push unless your lead says so.

    *Go home early:* review 2 came back clean? Skip to "Wrap it up".

24. **Sort** review 2's findings, same four reads.
25. **Hand it to your lead,** the whole list in one message, and wait for
    their answer.
26. **Do what they say.** If that means changing code, it's block 3, and it
    happens only because your lead said so: fix, test, prove any new test can
    fail, commit, local green, push, CI green. Reply on each of Copilot's
    threads with what you did, and resolve it.

*Wrap it up*

27. **Hand off.** Check the prompt's "Done when" list. Then send your lead a
    **"ready to merge" report**: the head SHA, Copilot reviews used, every
    finding with what your lead decided and what you did, anything on "Done
    when" that isn't met, and a squash title and message (the PR's title plus
    `(zerobull-consulting/dagger#<PR>)`, never GitHub's default `(#<PR>)`; the
    message is everything in the PR description above `## Prompt`). Your lead
    merges, files what they decided to file, closes the issue, and tells you.
28. **After the merge:** unsubscribe from the PR if you subscribed. Report
    review rounds used and what changed. Go home. You earned it.

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

**Why the lead decides scope after the first green CI.** Splitting large work
where it splits naturally cuts findings more than anything else. Deciding
once, before Copilot sees it, stops fixes during the rounds from reopening it;
the review cap limits growth after that. The session gives the size and how it
feels about the scope, because the lead, not the session, files the split. The
size counts every file, tests and generated ones included, from committed
`HEAD` (a binary file counts as 0).

**Why the explicit review range and the branch check.** On
zerobull-consulting/dagger#182, `/code-review` reviewed an earlier PR's
commits and left `HEAD` detached.

**Why blocks of self-review, each round through the lead.** A second
self-review checks the fixes from the first: in the round of
zerobull-consulting/dagger#180 to zerobull-consulting/dagger#190 it caught
real problems in fixes on zerobull-consulting/dagger#186 (a duplicated
deadline, a ref check's scope) and zerobull-consulting/dagger#188 (an
unfiltered publish-run query), and no PR needed a third Copilot review. Each
block is one round, and another round is the lead's call: a loop ("go back to
Review, five rounds, then ask") is where sessions get lost, and the lead
already sees every finding. Copilot's findings go to the lead right before
block 2, so its fixes get a self-review before the one push. Every round goes
through the lead so the session's instruction stays the same each time: sort,
hand over, do what the lead says. Block 1 runs before the draft so CI only
spends on reviewed code. Fixes are committed before the next round because the
review range compares commits only (zerobull-consulting/dagger#193).

**Why the check-ins.** A review can land without waking the session. Three
sessions in that round went idle beside a review that had already arrived
(zerobull-consulting/dagger#180, zerobull-consulting/dagger#182,
zerobull-consulting/dagger#188) and sat until someone nudged them.

**Why the lead decides on findings and files the issues.** One leads issue per
PR meant twelve of them piled up untriaged. One rolling leads issue fixed
that, but it was hard to explain to a session working on a single task. Later,
sessions filed some findings, fixed others under a list of exceptions, and
asked about the rest, which was more than a session could reliably work
through. Deciding what a finding is worth, and what it belongs with, takes a
view across tasks, which the lead has and the session doesn't. The ladder in
"Deciding on a finding" brings back known-gap tests: pinning a gap with a test
kept it from drifting unnoticed, and the fix later flips the test (the
`TestKnownGap…` tests in `cmd/cacheservice/server/drain_test.go` are
examples).

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
different wording each time, makes the second round visibly not the first. The
rules stay plain so the limits are never ambiguous. The "go home early" exits
reward a PR that's going well (zerobull-consulting/dagger#198).

## Conventions for the whole prompt

- Say when to **stop and ask** rather than decide: design decisions, a missing
  prerequisite, or a Dagger engine change.
- **Measure before choosing a number,** and put the measurement in the PR.
- **Decisions that are the requester's are settled before the session starts,**
  and written into the prompt as decided: credentials, Dagger engine changes,
  ruleset changes, design forks. A question that only the requester can answer
  stalls a session that hits it.
- **The lead always merges.** Before merging, it checks the report against
  the PR: the ruleset's required checks passed on the head being merged; at
  most two Copilot reviews that ran (a re-requested run that failed doesn't
  count), plus one for each push after review 2 that was a CI fix or a block
  3 the lead asked for, and nothing else pushed after review 2; and the
  prompt verbatim in the PR. The steps don't state the cap as a number, so
  this is where it's checked.
- **The lead decides on every finding,** using [Deciding on a
  finding](#deciding-on-a-finding), and files every issue: on its own, or
  with similar ones.
- **Run items one at a time,** in the order the requester set. Before each
  launch, refresh `feature/remote-cache`, and carry what the last session hit
  into the next prompt's "Heads up" (section 11). Running items in parallel
  turned out to be hard.

## Starting a session

An orchestrator creates the session with the standing authorization as its
appended system prompt and the full prompt, section 12 included, as its
first message. A human pastes both into the first message, authorization
first. An orchestrator lead then checks in on the session about 10 minutes
after launch, and every 20 minutes while it works; a human lead hears from
the session when it stops.

The session copies the whole prompt, verbatim, into the draft PR it opens,
but never the standing authorization, even when a human pasted both into the
first message. A follow-up PR from the same prompt links to the original's
instead of copying it.

## Changing this file

- The template changes by PR, like any other file, into `feature/remote-cache`.
- The PR says what changed in the process and why.
- A change to section 12 changes every prompt from then on. Put the reason in
  "Why the steps look like this".
- There's no version marker. Every PR carries its full prompt, section 12
  included, in its description (a follow-up PR links to the one that does),
  so each PR already records the exact process it followed.
- The draft in zerobull-consulting/dagger#95's body and the working
  agreement's comments on zerobull-consulting/dagger#1 are history; this file
  supersedes them.
