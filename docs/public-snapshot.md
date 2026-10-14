# Local public snapshot

The private repository keeps its full history. `scripts/public_snapshot.py`
exports regular files from one commit into a new local repository with one commit
and no remote. It never pushes. Publication and public repository settings are
separate owner steps. The intended public repository is share-only, with pull
requests off and no CI.

Install Git, gitleaks, and trufflehog locally. The builder uses `/usr/bin/python3`
and the Python standard library. On macOS, Homebrew provides both scanners.

```sh
/usr/bin/python3 scripts/public_snapshot.py \
  --commit HEAD \
  --output /absolute/new-snapshot-folder \
  --denylist /absolute/private-identifiers.txt \
  --author-name 'Public Author' \
  --author-email 'public@example.invalid'
```

The output folder must not exist. The denylist stays outside the repository.
Supply at least one nonblank line. Each line is a literal, case-insensitive byte
substring checked against exported filenames, file contents, and the supplied
commit identity. Any match stops the build. The repository ships no identifier
list. Use the real private list before publication; a harmless fixture proves
only that the builder runs.

`scripts/public_snapshot_files.json` explicitly lists every kept file and its
reason. It also records reasons for excluded tracked files. Files absent from
`keep` never ship. The selected commit supplies both the list and the file
contents; uncommitted changes are not exported. Symlinks and submodules cannot
ship. Update the list deliberately when adding public files.

The builder writes the short source commit to `VERSION`. Torii reads it when
Git is unavailable. Git tar and zip archives substitute the same value through
`.gitattributes`. Archives can then identify their code in the service log.

The builder and its tests ship so readers can reproduce an export. It excludes
private investigations, old plans, handoff and account notes, standup pages,
and the private GitHub workflow. Operational account instructions remain in
`docs/operations.md`. The product name is Torii and the LICENSE holder is
"Torii contributors". Only the public repository's location awaits the owner's
choice.

Both scanners must succeed on the export before Git initialization. Gitleaks
redacts findings. TruffleHog runs with credential verification and update checks
disabled and fails on findings or scan errors. Scanner output is suppressed to
keep detected values out of logs. On failure, the builder leaves partial output
for inspection and prints a generic failure. Use a fresh folder for a retry, or
move the inspected partial folder to Trash. Do not publish it.

On success, stdout contains a JSON report with source and snapshot commits,
scanner results, and every kept or omitted file with its reason. Save that report
outside the export. The snapshot author and committer use only the supplied
identity. The short source commit in `VERSION` is the only source history identifier
copied. Private history, commit messages, hooks, and remotes stay private.
Local global Git config and templates are disabled for the new commit.
