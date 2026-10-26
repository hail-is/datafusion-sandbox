# Issue tracker: GitHub

Issues and PRDs for this repo live as GitHub issues on `hail-is/datafusion-sandbox`. Use the `gh` CLI for all operations.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --json title,body,labels,comments`. Without a terminal, `--comments` prints only the comments, sometimes nothing.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Infer the repo from `git remote -v` — `gh` does this automatically when run inside a clone.

Where GraphQL is blocked, as in a [cloud session](../cloud-sessions.md), `gh issue` and `gh pr` fail; use the REST API through `gh api`:

- **Read an issue**: `gh api repos/hail-is/datafusion-sandbox/issues/<number> --jq '{title, body, labels: [.labels[].name]}'` and `gh api repos/hail-is/datafusion-sandbox/issues/<number>/comments --jq '.[].body'`
- **Comment**: `gh api repos/hail-is/datafusion-sandbox/issues/<number>/comments -f body="..."`
- **Apply / remove labels**: `gh api repos/hail-is/datafusion-sandbox/issues/<number>/labels -f "labels[]=..."` / `gh api -X DELETE repos/hail-is/datafusion-sandbox/issues/<number>/labels/<label>`
- **Open a pull request**: `gh api repos/hail-is/datafusion-sandbox/pulls -f title="..." -f head="<branch>" -f base=main -F draft=true -f body="..."`

## Pull requests as a triage surface

**PRs as a request surface: no.** This repo is public but has no external contributors, so open PRs are in-flight work rather than incoming requests. `/triage` reads only issues.

_(Set to `yes` if this repo ever starts treating external PRs as feature requests.)_ When set to `yes`, PRs run through the same labels and states as issues, using the `gh pr` equivalents:

- **Read a PR**: `gh pr view <number> --comments` and `gh pr diff <number>` for the diff.
- **List external PRs for triage**: `gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments` then keep only `authorAssociation` of `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, or `NONE` (drop `OWNER`/`MEMBER`/`COLLABORATOR`).
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label`/`--remove-label`, `gh pr close`.

GitHub shares one number space across issues and PRs, so a bare `#42` may be either — resolve with `gh pr view 42` and fall back to `gh issue view 42`.

## When a skill says "publish to the issue tracker"

Create a GitHub issue.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --json title,body,labels,comments`.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a single issue with **child** issues as tickets.

- **Map**: a single issue labelled `wayfinder:map`, holding the Notes / Decisions-so-far / Fog body. `gh issue create --label wayfinder:map`.
- **Child ticket**: an issue linked to the map as a GitHub sub-issue with `scripts/gh-link sub <map> <child>...`. Where sub-issues aren't enabled, add the child to a task list in the map body and put `Part of #<map>` at the top of the child body. Labels: `wayfinder:<type>` (`research`/`prototype`/`grilling`/`task`), plus `wayfinder:claimed` once claimed.
- **Blocking**: native issue relationships where available, recorded with `scripts/gh-link blocked-by <issue> <blocker>...`; otherwise a `Blocked by: #<n>, #<n>` line at the top of the child body. A ticket is unblocked when every issue it lists is closed.
- **Frontier query**: with native relationships, `gh api --paginate "repos/hail-is/datafusion-sandbox/issues/<map>/sub_issues" --jq '.[] | select(.state == "open" and .issue_dependencies_summary.blocked_by == 0 and all(.labels[]; .name != "wayfinder:claimed")) | .number'` prints the frontier in map order (`blocked_by` counts only open blockers); the first wins. Otherwise, read the child numbers and their order from the map's task list, check each with `gh issue view <n> --json state,labels,body`, and drop any that is closed, has an open `Blocked by` issue, or has the `wayfinder:claimed` label; the first remaining child in task-list order wins.
- **Claim**: `gh issue edit <n> --add-label wayfinder:claimed` — the session's first write.
- **Resolve**: `gh issue comment <n> --body "<answer>"`, then `gh issue close <n>`, then append a context pointer (gist + link) to the map's Decisions-so-far.

Note: the `wayfinder:*` labels are not created up front. Create them on first use with `gh label create`.
