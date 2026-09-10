# Contributing to WNL ICB Analytics dbt

This guide covers local setup and the pull request workflow.

Use the [dbt onboarding handbook](https://dbt-onboarding.vercel.app/) to learn
the project, then keep [Project conventions](PROJECT_CONVENTIONS.md) beside
you while changing models.

## Set up locally

Prerequisites (Windows):

- **Git for Windows:** [Download from git-scm.com](https://git-scm.com/download/win).
  Version 2.34 or later is needed for SSH commit signing.
- **Access to Snowflake** with the ANALYST role. You will need your account
  identifier, username, role (`ANALYST`) and warehouse (usually `NCL_ANALYTICS_XS`).
  To find them, log in to Snowflake, select your name in the bottom-left corner,
  then "Connect a tool to Snowflake". Ask your team lead if you don't have access.
- **A text editor:** We recommend [VS Code](https://code.visualstudio.com/).

### Step 1: Allow PowerShell to run scripts

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

### Step 2: Clone the repository

```powershell
git clone https://github.com/wnl-icb-analytics/dbt-analytics
cd dbt-analytics
```

### Step 3: Run the setup script

```powershell
.\start_dbt.ps1
```

The script does the rest:

- configures git hooks and checks commit signing
- installs the dbt Fusion engine if it is missing
- installs `uv` and syncs the Python tooling for `scripts/`
- asks for your Snowflake account, user, role, warehouse and auth method, then
  writes `.env` (first run only)
- installs dbt packages

The VS Code workspace runs it every time you open a terminal, so you rarely need
to run it by hand after this.

### Step 4: Verify

```powershell
dbt debug
```

With browser SSO (the default), your browser opens for Snowflake authentication.
Look for "All checks passed!" in the output.

## Set up manually

Use this only if the setup script cannot run on your machine. It reproduces what
`start_dbt.ps1` does.

**1. Install the dbt Fusion engine.** dbt is not a Python package in this
project; it is the Fusion binary, installed to `%USERPROFILE%\.local\bin`:

```powershell
irm https://public.cdn.getdbt.com/fs/install/install.ps1 | iex
```

**2. Install uv** *(optional)*. Only needed for the Python helper scripts in
`scripts/`:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
uv sync
```

**3. Configure git hooks:**

```powershell
git config core.hooksPath .githooks
```

**4. Configure the Snowflake connection:**

```powershell
cp env.example .env
```

Then fill in `.env`:

```bash
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your.username
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_ROLE=your-role
```

Leave it there for **browser SSO** (the default). For a **PAT**, set
`SNOWFLAKE_PAT` (Fusion authenticates via `programmatic_access_token`). For an
**account password**, set `SNOWFLAKE_PASSWORD` (used with MFA). `profiles.yml`
picks the authenticator from whichever you set. Fusion loads `.env` itself.

**5. Install packages and verify:**

```powershell
dbt deps
dbt debug
```

## Use GitHub Codespaces

Codespaces installs everything on creation (Fusion, Python tooling, packages) and
authenticates with your Codespaces secrets. It needs no local install or `.env`. See
**[Developing in GitHub Codespaces](docs/codespaces.md)** for the walkthrough:
which secrets to add, scoping them to the repo, and how auth works.

## Use the helper scripts

Two scripts in the project root make development easier:

| Script | Description |
|--------|-------------|
| `.\start_dbt.ps1` | Installs dbt Fusion, configures git hooks, sets up and loads `.env`, syncs Python tooling, installs dbt packages (auto-runs on terminal open) |
| `.\build_changed.ps1` | Builds only models changed on your branch |

**build_changed flags:**
- `-u` include upstream dependencies
- `-d` include downstream dependents
- `-r` run only (skip tests)
- `-t` test only (skip run)

Example: `.\build_changed.ps1 -u -d` builds changed models with upstream and
downstream dependencies.

## Change a model

Read [Project conventions](PROJECT_CONVENTIONS.md) before editing SQL. It defines
model design, layer boundaries, ownership, documentation, tests and data safety.
Check downstream impact with `dbt ls -s model_name+`, then change the SQL and
related YAML together.

Validate as you work:

```powershell
dbt compile -s model_name
dbt build -s model_name
# When the change can affect consumers:
dbt build -s model_name+
```

`dbt show` executes SQL and returns its result. Use it sparingly through a coding
agent, and only when the query is designed to return a high-level,
non-identifying aggregate. Do not use it to preview model rows. When validation
needs row-level inspection, give the user a ready-to-run Snowflake-native query
for an approved human-controlled tool and ask only for the non-identifying
aggregate or confirmation needed. Apply the same rule to ad hoc queries and
failing-test SQL.

Build downstream only where the change can affect consumers. If the full
selection is too large, build direct children and state the limit in the pull
request. For changed models across a branch, use `.\build_changed.ps1`; add `-d`
to include downstream consumers.

## Setting Up Commit Signing

This repository requires all commits to be cryptographically signed.

### Why commits are signed

Commit signing proves that commits actually came from you, not someone impersonating you. GitHub will show a "Verified" badge on signed commits.

### Configure signing

**1. Generate an SSH key:**

```bash
ssh-keygen -t ed25519 -C "your.email@nhs.net"
```

**Important**: Use the same email address that you use for your GitHub account.

- Press Enter to accept the default file location (`~/.ssh/id_ed25519`)
- Enter a passphrase when prompted (recommended for security)

**2. Create an allowed signers file (for local signature verification):**

```bash
echo "your.email@nhs.net $(cat ~/.ssh/id_ed25519.pub)" > ~/.ssh/allowed_signers
```

**3. Configure Git to use SSH signing:**

```bash
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
git config --global gpg.ssh.allowedSignersFile ~/.ssh/allowed_signers
git config --global user.email "your.email@nhs.net"
git config --global user.name "Your Name"
```

**4. Add the SSH key to GitHub as a signing key:**

Copy your public key:
```bash
Get-Content ~/.ssh/id_ed25519.pub | Set-Clipboard
```

Then:
1. Go to [GitHub Settings, SSH and GPG keys](https://github.com/settings/keys)
2. Click "New SSH key"
3. **Important**: Select "Signing Key" as the key type (not "Authentication Key")
   - There's a dropdown that defaults to "Authentication Key"
   - You must change this to "Signing Key"
4. Paste your public key and give it a descriptive title (e.g., "Work Laptop Signing Key")
5. Click "Add SSH key"

### Verify the setup

Create a test commit:

```bash
git commit --allow-empty -m "test: verify signed commits"
```

If local signature verification reports a line-ending error, convert the file
to LF:

```bash
$file = "$env:USERPROFILE\.ssh\allowed_signers"; $content = [System.IO.File]::ReadAllText($file); [System.IO.File]::WriteAllText($file, $content.Replace("`r`n", "`n"), [System.Text.Encoding]::UTF8); Write-Host "Line endings converted from CRLF to LF"
```

Check the signature:

```bash
git log --show-signature -1
```

You should see "Good signature" in the output.

## Follow the development workflow

### Branch protection rules

The `main` branch is protected:
- **No direct commits:** All changes must go through a pull request
- **Signed commits required:** All commits must be signed
- **No force pushes:** History cannot be rewritten

### Create a branch

Never work directly on main. Always create a new branch:

```bash
# Create and switch to a new feature branch
git switch -c feat/your-feature-name

# Or for bug fixes
git switch -c fix/your-bug-fix

# Or for documentation
git switch -c docs/your-doc-update
```

### Write the commit message

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>: <description>

[optional body]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding or correcting tests
- `chore`: Changes to build process or tools

**Examples:**
```bash
git commit -m "feat: add patient demographics staging model"
git commit -m "fix: correct join logic in int_appointments"
git commit -m "docs: update setup instructions in CONTRIBUTING"
```

### Create a pull request

1. **Push your branch:**
   ```bash
   git push -u origin feat/your-feature-name
   ```

   The `-u origin branch-name` creates the branch on GitHub and links it to your local branch. After this first push, you can use just `git push` for subsequent updates.

2. **Create PR on GitHub:**
   - Go to the repository on GitHub
   - Select **Pull requests**, then **New pull request**.
   - Select your branch
   - Explain why the change exists. One clear sentence can be enough for a small
     change; add changed behaviour, checks or review questions when they help
   - Reference any related issues (e.g., "Fixes #123")

   This repository is public. Do not include credentials, patient- or
   person-level data, identifying values, row-level output or screenshots of
   real data. Suspected disclosure is a critical blocking finding. Use aggregate
   or non-identifying validation evidence. High-level counts, rates,
   distributions and validation totals are not person-level data when they
   cannot identify an individual. A data-safety finding must be resolved before
   merge even though CodeRabbit does not submit a formal request-changes review.

3. **Wait for review:**
   - Fast checks compile the project and check references, descriptions,
     declared test coverage and ownership
   - CodeRabbit reviews the change against the project conventions
   - Address any feedback from reviewers
   - Once approved, select **Merge when ready**. The merge queue builds changed
     models and runs their data tests in Snowflake development before merge

Reviews focus on changed behaviour and its effect on the existing model
contract. Pre-existing design debt is not a merge condition unless the change
worsens it, depends on it, or cannot be safe without resolving it. Wider
redesign may be recorded as `follow-up (non-blocking):` without expanding the
scope of the pull request.

### Keep the branch up to date

```bash
# Switch to main and pull latest changes
git switch main
git pull

# Switch back to your feature branch
git switch feat/your-feature-name

# Merge main into your branch
git merge main
```

If you encounter merge conflicts, Git will tell you which files have conflicts. Open those files, look for conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`), resolve them, then:

```bash
git add <resolved-files>
git commit
```

### Use Git stash

If you need to switch branches but have uncommitted changes:

```bash
# Save your current work
git stash

# Switch branches and do other work
git switch main
git pull

# Go back to your feature branch
git switch feat/your-feature-name

# Restore your saved changes
git stash pop
```

## Use the pre-commit hooks

Pre-commit hooks run automatically when you commit and will:
- Validate commit message format
- Check for trailing whitespace
- Ensure files end with newlines
- Fix common formatting issues

If a hook fails, fix the reported issue and commit again.

## Work with dbt packages

This repository commits `dbt_packages/` to ensure consistent package versions. When `dbt deps` shows changes in `dbt_packages/`, only commit if you're intentionally updating packages.

## Fix common issues

**SSH signing fails:**
- Check Git version: `git --version` (need 2.34+)
- Verify SSH key matches the one on GitHub
- Make sure you selected "Signing Key" not "Authentication Key"

**Python command not found:**
- Use `py` instead of `python`
- Or install Python 3.11 and select it in your terminal or editor

**PowerShell won't run scripts:**
- Run `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`

**dbt authentication fails:**
- Check your `.env` file has correct values
- Try running `dbt debug` to see detailed error

## Get help

- Check existing [GitHub Issues](https://github.com/wnl-icb-analytics/dbt-analytics/issues)
- Work through the courses and handbook at [dbt-onboarding.vercel.app](https://dbt-onboarding.vercel.app/)
- Read [Working with Sources](docs/working-with-sources.md) for the source generation pipeline
- Create a new issue with details about your problem
