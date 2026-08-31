# Repository Instructions for Agents

## Implementation Discipline

These rules govern task execution and changes to code, configuration, tests, or documentation.

- For inquiry tasks such as explaining, reviewing, diagnosing, or planning, report findings instead of changing files unless the user asks for edits.
- Before changing behavior, read the relevant existing code, tests, and local guidance so the change fits the current implementation.
- Preserve existing external interfaces unless the request requires changing them. If a change affects APIs, CLI flags, configuration keys, environment variables, ROS topics, services, actions, parameters, launch arguments, frame IDs, Docker ports or volumes, or model input/output schemas, mention it explicitly.
- Do not silently choose between ambiguous interpretations. State assumptions when they affect the implementation or verification path; if multiple interpretations exist, list the options briefly and choose the narrowest reasonable path, or ask when the ambiguity would make the change risky.
- Prefer the smallest solution that satisfies the request. If a simpler approach exists, say so. Do not add speculative features, abstractions, configurability, or error handling for impossible scenarios.
- If a change is becoming much larger than the problem requires, pause and simplify before continuing.
- Keep changes focused on the user's request. Do not refactor, reformat, rename, or rewrite adjacent code unless it is required for the requested change.
- Ask for confirmation before destructive or hard-to-reverse operations, or any material expansion of the requested scope.
- Match the existing style and local patterns, even when another style would also be valid.
- Do not revert or overwrite user changes unless explicitly requested.
- Clean up unused imports, variables, functions, or files introduced by your own changes. Mention unrelated pre-existing dead code when useful, but do not delete it unless explicitly asked.
- For multi-step tasks, keep the plan brief and tied to verifiable outcomes.
- Before finishing, run the most focused available validation for the changed behavior, such as a relevant test, typecheck, lint, or configuration validation. Prefer targeted checks before repo-wide commands, and continue until the change is verified or a blocker is clear.

## Conventional Commits

All commit messages MUST follow the Conventional Commits specification.

This rule only governs the commit message text. The user decides which changes belong in each commit.

Use one of the following types, intentionally aligned with the repository's `conventional-pre-commit` validation:

`build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, or `test`.

- Use the format `type(scope): description`, where the scope is optional.
- Use a lowercase type.
- Write the description as a concise imperative statement without a trailing period.
- Mark breaking changes with `!` before the colon or with a `BREAKING CHANGE:` footer.
- If `!` is used, the description should describe the breaking change; a `BREAKING CHANGE:` footer may still be added for extra detail.
- For non-trivial commits, add a concise body after a blank line explaining what changed and why it matters.
- Mention relevant behavior, configuration, dependency, launch, model, or interface changes when useful.
- Do not repeat the diff or write an exhaustive changelog.
- Omit the body for trivial commits where the description is sufficient.
- Keep system-generated merge commit messages as generated.
- Do not introduce additional commit types unless explicitly requested by the user or deliberately added to the repository policy.
