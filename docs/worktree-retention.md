# Worktree retention controls

Closed-job cleanup accepts worktrees with initialized submodules. Before retirement,
it saves source from the parent and each recursive initialized submodule under
`results/task-N/worktree-salvage/salvage-*/`. The staging and Trash paths use the
same preservation code. Ordinary dirty parent worktrees retain the existing guard.
The separate `close_clears_space` round-save mode retains its existing submodule
hold. Its automatic snapshot and rebuild format is outside these two paths.

Each preservation directory contains a manifest and one numbered directory per
repository. The directory contains binary patches for the net, staged, and
unstaged changes. `untracked.list` contains NUL-separated names. `untracked/`
contains those files and symbolic links. A verified `commits.bundle` contains
commits unreachable from remote refs, including detached submodule HEADs.
Remote refs are local evidence. Cleanup does not fetch from the network.
Closed-job cleanup retains its existing disposable-file policy for ignored files.
The external sweep keeps trees with ignored files, nested repositories, or
reflog-only commits because those sources are outside its preservation format.

Preservation failure or a changed source snapshot keeps the original tree.
Hidden-file flags, active Git operations, locks, and unenumerated submodule
metadata and submodule reflog-only commits also keep the tree. Nonempty submodule folders omitted by Git's initialized
submodule listing keep the tree. Hold checks run before preservation, which builds
in worker temporary storage and publishes only a completed salvage directory.
If temporary storage and results are on different filesystems, publication fails
closed and the original worktree stays in place. Salvage records are retained
results. This change does not introduce an automatic policy to remove them.

Worker scratch cleanup records the common Git directories of linked worktrees
before it detaches scratch folders. After byte removal, it retires only Git
administrative entries whose checkout pointers are inside that worker's deleted
scratch roots, under the repository lock. Other missing checkouts retain their
registrations and reflogs. A failed retirement remains pending for retry; a
missing common Git directory counts as complete.

## External worktree sweep

`python3 -m coordinator.external_worktrees` reports registered worktrees below
explicit scratch roots. The command does not start the service or access Telegram.
It opens the coordinator database read-only. It does not discover arbitrary
directories or classify unrelated projects as Torii work.

Set the following paths for the configured project before running a report:

```sh
python3 -m coordinator.external_worktrees \
  --state-dir "$TORII_STATE_DIR" \
  --repository "$PROJECT_REPOSITORY" \
  --root "$PROJECT_SCRATCH_ROOT" \
  --idle-days 7 \
  --report "$REPORT_DIRECTORY/external-worktrees.json"
```

`--repository` and `--root` can repeat. A scratch root is an owner-approved
directory containing Torii-created worktrees. Roots and their main repository
checkouts are never retirement candidates themselves. Managed `worktrees/task-N`
folders retain the closed-job cleanup policy. Native history and account homes
remain protected.

The default idle period is seven days. Eligibility requires old file and Git
metadata modification times. Related closed jobs and finished workers must also
be old enough. Open jobs, unfinished workers, queued file delivery, protected
names or branches, Git operations, and locks block retirement. Owner association
uses saved task paths, worker paths, and `torii/task-N` branches. No matching owner
uses the same file-age and process guards.

Level A protection checks current job notes and file references in the configured
repositories. `--reference-root` adds directories to search. A reference outside
the candidate keeps the tree. Search failure also keeps the tree. If `rg` is
unavailable, a bounded Python file scan performs the reference check. Apply
repeats these checks after preservation. Full paths in the protected-name list protect
the named tree and its contents.

The command scans live open-file paths through macOS `lsof`. A live process in
the checkout or its Git directory blocks retirement. A failed process scan keeps
the tree. A held repository lock skips the candidate. The default limits are
120 seconds per worktree and 900 seconds for the sweep. `--root-seconds` and
`--total-seconds` configure those limits. Reports record skipped candidates.

`--apply` enables retirement and requires `--protected-names FILE`. The file uses
one protected name per line, optionally followed by tab-separated source fields.
Use the configured owner protection list. Apply preserves source in
`results/external-worktrees/salvage-*/`, then repeats the eligibility and source
checks before `git worktree remove --force`, which removes that checkout's own
registration. The command never prunes unrelated registrations. Preservation
failure keeps the original tree. The report links each completed salvage manifest.
The report path must be outside the scratch roots.

The sweep is opt-in. This change does not install a schedule or enable apply.
