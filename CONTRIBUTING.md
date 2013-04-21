# Contributing to Snowplow

So you want to contribute to Snowplow? Fantastic! Here's a brief overview on
how best to do so.

## Support request?

If you are having trouble setting up or running Snowplow, then the best place to get help is on the [SnowPlow Google Group](https://groups.google.com/forum/#!forum/snowplow-user) (aka `snowplow-user`).

Posting your problem there ensures more people will see it and you should get support faster than creating a new issue on GitHub. Please do create a new issue on GitHub if you think you've found a bug though! 

## What to change

Here's some examples of things you might want to make a pull request for:

* New features
* Bugfixes
* Inefficient blocks of code

If you have a more deeply-rooted problem with how the program is built or some
of the stylistic decisions made in the code, it's best to
[create an issue](https://github.com/snowplow/snowplow/issues/new) before putting
the effort into a pull request. The same goes for new features - it might be
best to check the project's direction, existing pull requests, and currently open
and closed issues first.

## Style

* Two spaces, not tabs
* Trailing newline at end of source files
* No editor-specific cruft in source code or `.gitignore`
* Code should follow our accepted style guides (coming soon)

Look at existing code to get a good feel for the patterns we use.

## Using Git appropriately

1. [Fork the repository](https://github.com/snowplow/snowplow/fork_select) to
your GitHub account
2. Create a *topical branch* - a branch whose name is succint but explains what
you're doing, such as "feature/storm-etl"
3. Make your changes, committing at logical breaks
4. Push your branch to your personal account
5. [Create a pull request](https://help.github.com/articles/using-pull-requests)
6. Watch for comments or acceptance

Please note - if you want to change multiple things that don't depend on each
other, make sure you check the master branch back out before making more
changes - that way we can take in each change seperately.
