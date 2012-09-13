---
layout: global
title: Contributing to Spark
---

The Spark team welcomes contributions in the form of GitHub pull requests. Here are a few tips to get your contribution in:

- Break your work into small, single-purpose patches if possible. It's much harder to merge in a large change with a lot of disjoint features.
- Submit the patch as a GitHub pull request. For a tutorial, see the GitHub guides on [forking a repo](https://help.github.com/articles/fork-a-repo) and [sending a pull request](https://help.github.com/articles/using-pull-requests).
- Follow the style of the existing codebase. Specifically, we use [standard Scala style guide](http://docs.scala-lang.org/style/), but with the following changes:
  * Maximum line length of 100 characters.
  * Always import packages using absolute paths (e.g. `scala.collection.Map` instead of `collection.Map`).
  * No "infix" syntax for methods other than operators. For example, don't write `table containsKey myKey`; replace it with `table.containsKey(myKey)`.
- Add unit tests to your new code. We use [ScalaTest](http://www.scalatest.org/) for testing. Just add a new Suite in `core/src/test`, or methods to an existing Suite.

If you'd like to report a bug but don't have time to fix it, you can still post it to our [issues page](https://github.com/mesos/spark/issues). Also, feel free to email the [mailing list](http://www.spark-project.org/mailing-lists.html).
