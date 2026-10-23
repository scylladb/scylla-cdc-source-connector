#!/usr/bin/env bash
set -euo pipefail
# Maven 3.10 adds repository metadata that Sonatype rejects; apache/maven#13388.
maven_version=3.9.16
archive="$RUNNER_TEMP/apache-maven-${maven_version}-bin.tar.gz"
curl --fail --location --silent --show-error \
  "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${maven_version}/apache-maven-${maven_version}-bin.tar.gz" \
  --output "$archive"
echo "831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6  $archive" | sha512sum --check
tar -xzf "$archive" -C "$RUNNER_TEMP"
echo "$RUNNER_TEMP/apache-maven-${maven_version}/bin" >> "$GITHUB_PATH"
