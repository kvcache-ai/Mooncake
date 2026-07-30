Introduction
============

liburing welcomes contributions, whether they be bug fixes, features, or
documentation additions/updates. However, we do have some rules in place
to govern the sanity of the project, and all contributions should follow
the guidelines in this document. The main reasons for the rules are:

1) Keep the code consistent
2) Keep the git repository consistent
3) Maintain bisectability

Coding style
============

Generally, all the code in liburing should follow the same style. A few
known exceptions exist, like syzbot test cases that got committed rather
than re-writing them in a saner format. Any change you make, please
follow the style of the code around you.

Commit format
=============

Each commit should do one thing, and one thing only. If you find yourself,
in the commit message, adding phrases like "Also do [...]" or "While in
here [...]", then that's a sign that the change should have been split
into multiple commits. If your change includes some refactoring of code to
make your change possible, then that refactoring should be a separate
commit, done first. That means this preparatory commit won't have any
functional changes, and hence should be a no-op. It also means that your
main commit, with the change that you actually care about, will be smaller
and easier to review.

Each commit must stand on its own in terms of what it provides, and how it
works. Lots of changes are just a single commit, but for something a bit
more involved, it's not uncommon to have a pull request contain multiple
commits. Make each commit as simple as possible, and not any simpler. We'd
much rather see 10 simple commits than 2 more complicated ones. If you
stumble across something that needs fixing while making an unrelated
change, then please make that change as a separate commit, explaining why
it's being made.

Each commit in a series must be buildable, it's not enough that the end
result is buildable. See reason 3 in the introduction for why that's the
case.

No fixup commits! Sometimes people post a change and errors are pointed
out in the commit, and the author then does a followup fix for that
error. This isn't acceptable, please squash fixup commits into the
commit that introduced the problem in the first place. This is done by
amending the fix into the original commit that caused the issue. You can
do that with git rebase -i <sha> and arrange the commit order such that
the fixup is right after the original commit, and then use 's' (for
squash) to squash the fixup into the original commit. Don't forget to
edit the commit message while doing that, as git will combine the two
commit messages into one. Or you can do it manually. Once done, force
push your rewritten git history. See reasons 1-3 in the introduction
series for why that is.

Commit message
==============

A good commit explains the WHY of a commit - explain the reason for this
commit to exist. Don't explain what the code in commit does, that should
be readily apparent from just reading the code. If that isn't the case,
then a comment in the code is going to be more useful than a lengthy
explanation in the commit message. liburing commits use the following
format:

Title

Body of commit

Signed-off-by: ```My Identity <my@email.com>```

That is, a descriptive title on the first line, then an empty line, then
the body of the commit message, then an empty line, and finally an SOB
tag. The signed-off-by exists to provide proof of origin, see the
[DCO](https://developercertificate.org/).

Example commit:

```
commit 0fe5c09195c0918f89582dd6ff098a58a0bdf62a
Author: Jens Axboe <axboe@kernel.dk>
Date:   Fri Sep 6 15:54:04 2024 -0600

    configure: fix ublk_cmd header check
    
    The previous commit is mixing private structures and defines with public
    uapi ones. Testing for UBLK_U_CMD_START_DEV is fine, CTRL_CMD_HAS_DATA
    is not. And struct ublk_ctrl_cmd_data is not a public struct.
    
    Fixes: 83bc535a3118 ("configure: don't enable ublk if modern commands not available")
    Signed-off-by: Jens Axboe <axboe@kernel.dk>
```

Since this change is pretty trivial, a huge explanation need not be given
as to the reasonings for the change. However, for more complicated
changes, better reasonings should be given.

A Fixes line can be added if this commit fixes an issue in a previous
commit. That kind of meta data can be useful down the line for finding
dependencies between commits. Adding the following to your .gitconfig:

```
[pretty]
	fixes = Fixes: %h (\"%s\")
```

and running ```git fixes <sha>``` will then generate the correctly
formatted Fixes line for the commit. Likewise, other meta data can be:

Link: https://somesite/somewhere

can be useful to link to a discussion around the issue that led to this
commit, perhaps a bug report. This can be a GitHub issue as well. If a
commit closes/solves a GitHub issue, than:

Closes: https://github.com/axboe/liburing/issues/XXXX

can also be used.

Each commit message should be formatted so each full line is 72-74 chars
wide. For many of us, GitHub is not the primary location, and git log is
often used in a terminal to browse the repo. Breaking lines at 72-74
characters retains readability in an xterm/terminal.

Pull Requests
=============

The git repository itself is the canonical location for information. It's
quite fine to provide a lengthy explanation for a pull request on GitHub,
however please ensure that this doesn't come at the expense of the commit
messages themselves being lacking. The commit messages should stand on
their own and contain everything that you'd otherwise put in the PR
message. If you've worked on projects that send patches before, consider
the PR message similar to the cover letter for a series of patches.

Most contributors seem to use GH for sending patches, which is fine. If
you prefer using email, then patches can also be sent to the io_uring
mailing list: io-uring@vger.kernel.org.

liburing doesn't squash/rebase-on-merge, or other heinous practices
sometimes seen elsewhere. Whatever sha your commit has in your tree is
what it'll have in the upstream tree. Patches are applied directly, and
pull requests are merged with a merge commit. If meta data needs to go
into the merge commit, then it will go into the merge commit message.
This means that you don't need to continually rebase your changes on top
of the master branch.

Testing changes
===============

You should ALWAYS test your changes, no matter how trivial or obviously
correct they may seem. Nobody is infallible, and making mistakes is only
human.

liburing contains a wide variety of functional tests. If you make changes
to liburing, then you should run the test cases. This is done by building
the repo and running ```make runtests```. Note that some of the liburing
tests test for defects in older kernels, and hence it's possible that they
will crash on an outdated kernel that doesn't contain fixes from the
stable kernel tree. If in doubt, building and running the tests in a vm is
encouraged.
