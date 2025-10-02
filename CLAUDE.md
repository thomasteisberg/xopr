# Project: xopr

## Project Description Open polar radar functions for reading data and creating
STAC catalogues

Our xopr project lives on github at https://github.com/thomasteisberg/xopr ;
when we reference a github issue or pull request using (for example) `gh #22`,
fetch it from our project directory.

## Best practices

  1. Code should be terse as possible-- the smaller the codebase, the easier it
     is to maintain.

  2. Explain what you are doing, and why. If you are less than 80% certain
     about what has been requested, ask clarifying questions.

  3. When updating our codebase, check for unused and orphaned functions and
     files. These should be deleted; see item 1 above!

When checking if a function or file is used, it is only used if it is tracked by git

## Git hygiene 

  1. Don't add or remove any new files for git tracking without first checking.

  2. When you modify a file which is currently tracked by git, git add it once
     it passes tests (and git add the tests too).

## Testing Practices

When creating a new function, python file, or module, create a unit test for
it. Check if the code passes the unit test; if not, continue debugging the code
until it does.

Our local testing data lives at /home/espg/Astera/opr_test_dataset_1/ , and is
what we reference when debugging the stac module.

If you are instructed to test documentation, build it and then deploy it (i.e.,
`uv run --extra docs myst start` and  `uv run --extra docs myst start`); provide
the link to the myst server.

## Performance Guidelines
- Prefer vectorized operations over loops
- Profile before optimizing

## Code Conventions

- Use spaces (no tabs)
- Function docstrings should follow numpydoc

## Instructions of Claude
- Follow code conventions, testing practices, best practices, and git hygiene
- When searching or researching, execute both default web search, and kagi search (via mcp)
