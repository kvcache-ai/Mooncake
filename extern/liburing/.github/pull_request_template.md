
<!-- Explain your changes here... -->

----
## git request-pull output:
```
<!-- START REPLACE ME -->

Generate your PR shortlog and diffstat with these commands:
   git remote add axboe-tree https://github.com/axboe/liburing
   git fetch axboe-tree
   git request-pull axboe-tree/master your_fork_URL your_branch_name

Then replace this with the output of `git request-pull` command.

<!-- END REPLACE ME -->
```
----
<details>
<summary>Click to show/hide pull request guidelines</summary>

## Pull Request Guidelines
1. To make everyone easily filter pull request from the email
notification, use `[GIT PULL]` as a prefix in your PR title.
```
[GIT PULL] Your Pull Request Title
```
2. Follow the commit message format rules below.
3. Follow the Linux kernel coding style (see: https://github.com/torvalds/linux/blob/master/Documentation/process/coding-style.rst).

### Commit message format rules:
1. The first line is title (don't be more than 72 chars if possible).
2. Then an empty line.
3. Then a description (may be omitted for truly trivial changes).
4. Then an empty line again (if it has a description).
5. Then a `Signed-off-by` tag with your real name and email. For example:
```
Signed-off-by: Foo Bar <foo.bar@gmail.com>
```

The description should be word-wrapped at 72 chars. Some things should
not be word-wrapped. They may be some kind of quoted text - long
compiler error messages, oops reports, Link, etc. (things that have a
certain specific format).

Note that all of this goes in the commit message, not in the pull
request text. The pull request text should introduce what this pull
request does, and each commit message should explain the rationale for
why that particular change was made. The git tree is canonical source
of truth, not github.

Each patch should do one thing, and one thing only. If you find yourself
writing an explanation for why a patch is fixing multiple issues, that's
a good indication that the change should be split into separate patches.

If the commit is a fix for an issue, add a `Fixes` tag with the issue
URL.

Don't use GitHub anonymous email like this as the commit author:
```
123456789+username@users.noreply.github.com
```

Use a real email address!

### Commit message example:
```
src/queue: don't flush SQ ring for new wait interface

If we have IORING_FEAT_EXT_ARG, then timeouts are done through the
syscall instead of by posting an internal timeout. This was done
to be both more efficient, but also to enable multi-threaded use
the wait side. If we touch the SQ state by flushing it, that isn't
safe without synchronization.

Fixes: https://github.com/axboe/liburing/issues/402
Signed-off-by: Jens Axboe <axboe@kernel.dk>
```

</details>

----
## By submitting this pull request, I acknowledge that:
1. I have followed the above pull request guidelines.
2. I have the rights to submit this work under the same license.
3. I agree to a Developer Certificate of Origin (see https://developercertificate.org for more information).
