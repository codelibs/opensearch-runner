# Surviving a foreign Log4j2 provider (issue #9)

## Problem

A cluster fails to start when `log4j-to-slf4j` is on the classpath, which
Spring Boot pulls in through `spring-boot-starter-logging`:

```
java.lang.ClassCastException: class org.apache.logging.slf4j.SLF4JLoggerContext
cannot be cast to class org.apache.logging.log4j.core.LoggerContext
    at org.opensearch.common.logging.LogConfigurator.configure(LogConfigurator.java:171)
    at org.codelibs.opensearch.runner.OpenSearchRunner.execute(OpenSearchRunner.java:568)
```

`LogConfigurator` casts the result of `LogManager.getContext(false)` to
`org.apache.logging.log4j.core.LoggerContext`. The Log4j2 provider lookup picks
the provider with the highest priority, and `log4j-to-slf4j` (15) outranks
log4j-core (10), so the cast fails. Because the choice is priority-based,
reordering dependencies does not change the outcome.

`disableESLogger()` does not help: it skips `LogConfigurator.configure`, but
node startup reaches the same cast through
`Node.<init>` -> `SearchRequestSlowLog.<init>` -> `Loggers.setLevel` ->
`Configurator.setLevel` -> `LoggerContext.getContext`. log4j-core must be the
active provider for a node to start at all.

## Decision

Detect the conflict in `build(String...)` and install log4j-core as the
provider through `LogManager.setFactory(new Log4jContextFactory())`, restoring
the previous factory in `close()`.

Replacing the factory at runtime works even after `LogManager` has been
initialized, and logging obtained before the replacement keeps working after
the original factory is restored.

The replacement is on by default. Users hit this in test setups they do not
fully control, and a library that needs no dependency surgery to run is the
point of this project. The runner already reconfigures the global Log4j2
context and replaces `System.out` and `System.err` through
`LogConfigurator.configure`, so this is not a new class of side effect.

### Alternatives rejected

- **Fail fast and document workarounds only.** Honest and side-effect free, but
  leaves users who cannot drop `log4j-to-slf4j` with no way forward.
- **Load OpenSearch in an isolated classloader.** The runner hands OpenSearch
  types such as `Client` back to callers, so OpenSearch must load in the
  caller's classloader, and log4j-api cannot be isolated on its own.

Reporting the cast upstream to OpenSearch remains worthwhile, and is
independent of this change.

## Design

`switchLoggerContextFactory()` runs once per `build(String...)`, after argument
parsing and before any filesystem or node setup, so an opt-out failure leaves
nothing behind. `restoreLoggerContextFactory()` runs from `close()` in a
`finally` block, and from `build` when a node fails to start.

Shared static state guarded by one lock keeps concurrent runners in the same
JVM correct: the factory is replaced only when no replacement is active, each
participating instance holds a reference, and the original factory is restored
when the last reference is released. Without the count, the first instance to
close would restore the foreign provider under a cluster still running.

`keepLoggerContextFactory()` (`-keepLoggerContextFactory`) opts out. With it, a
foreign provider raises an `OpenSearchRunnerException` naming the active
factory and listing workarounds, replacing the opaque `ClassCastException`.

The replacement is applied regardless of `disableESLogger`, since the
requirement comes from node startup rather than from log configuration.

## Testing

`OpenSearchRunnerLoggerContextFactoryTest` installs a `LoggerContextFactory`
returning `SimpleLoggerContext`, a non-core context from log4j-api, which
reproduces the reported `ClassCastException` without adding a dependency on
`log4j-to-slf4j`. The tests assert that a cluster starts with log4j-core
active, that the foreign factory is restored on close, and that the opt-out
raises a message naming the factory and a workaround.

Verified separately against the real reported setup: `log4j-to-slf4j` 2.25.4,
`slf4j-api` 2.0.17 and `logback-classic` 1.5.18 on the classpath, where an
unmodified caller now starts a cluster.
