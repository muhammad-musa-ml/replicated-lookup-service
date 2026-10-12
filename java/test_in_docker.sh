#!/bin/sh
# Run the Java tests without a local JDK. From the repository root:
#   docker run --rm -v "$PWD":/src -v rls-gradle:/home/gradle/.gradle gradle:9.8.1-jdk21 sh /src/java/test_in_docker.sh
set -e
cd /src/java
gradle --no-daemon test
grep -ho 'tests="[0-9]*" skipped="[0-9]*" failures="[0-9]*" errors="[0-9]*"' build/test-results/test/*.xml
