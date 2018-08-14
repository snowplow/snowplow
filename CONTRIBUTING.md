# Contributor guide

The referer-parser library is maintained by the pipeline team at Snowplow Analytics and improved on by
external contributors for which we are extremely grateful.

## Getting in touch

### Community support requests

First and foremost, please do not log an issue if you are asking a general question, all of our community support requests
go through our Discourse forum: https://discourse.snowplowanalytics.com/.

Posting your problem there ensures more people will see it and you should get support faster than creating a new issue on
GitHub. Please do create a new issue on GitHub if you think you've found a bug or if you would like to submit a feature
request though!

## Issues

### Creating an issue

The project contains an issue template which should help guiding you through the process. However, please keep in mind
that support requests should go to our Discourse forum: https://discourse.snowplowanalytics.com/ and not GitHub issues.

It's also a good idea to log an issue before starting to work on a pull request to discuss it with the maintainers.

### Working on an issue

If you see an issue you would like to work on, please let us know in the issue! That will help us in terms of scheduling and
not doubling the amount of work.

If you don't know where to start contributing, you can look at
[the issues labeled `good first issue`](https://github.com/snowplow-referer-parser/scala-referer-parser/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22).

## Pull requests

These are a few guidelines to keep in mind when opening pull requests, there is a GitHub template that reiterates most of the
points described here.

### Commit hygiene

We keep a strict 1-to-1 correspondance between commits and issues, as such our commit messages are formatted in the following
fashion:

`Add issues description (closes #1234)`

for example:

`Introduce an error ADT (closes #1234)`

### Writing tests and running tests

Whenever necessary, it's good practice to add the corresponding unit tests to whichever feature you are working on.

Then you can run `sbt test` to check they are working properly.

### Feedback cycle

Reviews should happen fairly quickly during weekdays. If you feel your pull request has been forgotten, please ping one
or more maintainers in the pull request.

### Getting your pull request merged

If your pull request is fairly chunky, there might be a non-trivial delay between the moment the pull request is approved and
the moment it gets merged. This is because your pull request will have been scheduled for a specific milestone which might or
might not be actively worked on by a maintainer at the moment.

### Contributor license agreement

We require outside contributors to sign a Contributor license agreement (or CLA) before we can merge their pull requests.
You can find more information on the topic in [the dedicated wiki page](https://github.com/snowplow/snowplow/wiki/CLA).
The @snowplowcla bot will guide you through the process.
