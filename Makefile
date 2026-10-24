SHELL := bash
.ONESHELL:

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

RELEASE_GUARD ?= scripts/release/guard.py
RELEASE_SIGNING_HELPER ?= $(dir $(RELEASE_GUARD))signing.py
RELEASE_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
.SHELLFLAGS := -eo pipefail -c
# Defaults are exported for recursive release recipes.
export RELEASE_SKIP_TESTS ?= false
export RELEASE_VERSION ?=

GET_VERSION_VERSION ?= 0.4.5
GET_VERSION := $(MAKEFILE_PATH)/bin/get-version

SCYLLA_VERSION ?= LATEST
KAFKA_PROVIDER ?= apache
KAFKA_VERSION ?= LATEST

# What the integration tests accept as an image tag: MAJOR.MINOR.PATCH with an optional
# -label or .Label suffix (4.1.0, 7.9.2, 3.4.1.Final, 2026.1.0-rc1).
FULL_VERSION_RE := ^[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9]+)?$$

.PHONY: github-release resolve-scylla-version resolve-kafka-version release-check-user release-check-tokens release-prepare release release-dry-run _release-prepare _release _release-dry-run test-release-signing

github-release: release-check-user
	python3 "$(dir $(RELEASE_GUARD))assets.py" publish

release-check-tokens:
	@python3 "$(dir $(RELEASE_GUARD))assets.py" check-tokens

release-check-user:
	@set -eu
	if [ "$${GITHUB_ACTIONS:-}" = true ]; then
		# The release environment authorizes workflow operators independently of the signer.
		exit 0
	fi
	if ! user=$$(gh api user --jq .login); then
		echo "Release requires GitHub user scylladb-publisher. Run gh auth login or gh auth switch." >&2
		exit 1
	fi
	if [ "$$user" != scylladb-publisher ]; then
		echo "Release requires GitHub user scylladb-publisher (current: $$user)." >&2
		exit 1
	fi

release-prepare: release-check-user
	@python3 "$(RELEASE_GUARD)" check
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _release-prepare

release: release-check-user release-check-tokens
	@python3 "$(RELEASE_GUARD)" check
	@python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _release

release-dry-run: release-check-user release-check-tokens
	@python3 "$(RELEASE_GUARD)" check
	@python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _release-dry-run

test-release-signing:
	python3 -m unittest discover -s scripts/release -p 'test_signing.py' -v

_release-prepare: release-check-user
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	NEXT_RELEASE=()
	if [[ -n "$${RELEASE_VERSION}" ]]; then
		if [[ ! "$${RELEASE_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$$ ]]; then
			echo "Release version must be X.Y.Z" >&2
			exit 1
		fi
		NEXT_RELEASE=("-DreleaseVersion=$${RELEASE_VERSION}")
	fi
	export MAVEN_OPTS
	baseline=$$(git rev-parse HEAD)
	python3 "$(RELEASE_GUARD)" run mvn -ntp -B release:prepare -DpushChanges=false "$${NEXT_RELEASE[@]}"
	python3 "$(RELEASE_SIGNING_HELPER)" save-prepared "$$baseline"

_release: release-check-user release-check-tokens
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	python3 "$(RELEASE_GUARD)" run mvn -ntp -B release:perform -Drelease.autopublish=true

_release-dry-run: release-check-user release-check-tokens
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	python3 "$(RELEASE_GUARD)" run mvn -ntp -B release:perform -Drelease.autopublish=false

$(GET_VERSION):
	@set -euo pipefail
	mkdir -p "$(MAKEFILE_PATH)/bin"
	tmp=$$(mktemp -d)
	trap 'rm -rf "$$tmp"' EXIT
	curl -sSfLo "$$tmp/get-version.zip" https://github.com/scylladb-actions/get-version/releases/download/v$(GET_VERSION_VERSION)/get-version_$(GET_VERSION_VERSION)_linux_amd64v3.zip
	unzip -o "$$tmp/get-version.zip" get-version -d "$(MAKEFILE_PATH)/bin" >/dev/null

# SCYLLA_VERSION: LATEST, PRIOR, LTS-LATEST, LTS-PRIOR, MAJOR.MINOR or a full version.
# Filters are the ones scylladb/java-driver uses, so both projects agree on what each alias means.
# Unlike java-driver there is no scylla-enterprise fallback: the tests only pull scylladb/scylla.
resolve-scylla-version: $(GET_VERSION)
	@set -euo pipefail
	gv() { "$(GET_VERSION)" --source dockerhub-imagetag "$$@" | tr -d '"'; }
	case "$(SCYLLA_VERSION)" in
		LATEST)     resolved=$$(gv --repo scylladb/scylla -filters '^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST') ;;
		PRIOR)      resolved=$$(gv --repo scylladb/scylla -filters '^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST-1') ;;
		LTS-LATEST) resolved=$$(gv --repo scylladb/scylla -filters '^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.1.LAST') ;;
		LTS-PRIOR)  resolved=$$(gv --repo scylladb/scylla -filters '^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST-1.1.LAST') ;;
		*)
			if [[ "$(SCYLLA_VERSION)" =~ $(FULL_VERSION_RE) ]]; then
				resolved="$(SCYLLA_VERSION)"
			elif [[ "$(SCYLLA_VERSION)" =~ ^[0-9]+\.[0-9]+$$ ]]; then
				resolved=$$(gv --repo scylladb/scylla -filters '^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and $(SCYLLA_VERSION).LAST')
			else
				echo "Unknown ScyllaDB version '$(SCYLLA_VERSION)'; expected LATEST, PRIOR, LTS-LATEST, LTS-PRIOR, MAJOR.MINOR or MAJOR.MINOR.PATCH" >&2
				exit 1
			fi
			;;
	esac
	if [[ ! "$$resolved" =~ $(FULL_VERSION_RE) ]]; then
		echo "Failed to resolve ScyllaDB '$(SCYLLA_VERSION)', got '$$resolved'" >&2
		exit 1
	fi
	echo "Resolved ScyllaDB $(SCYLLA_VERSION) to $$resolved"
	if [[ -n "$${GITHUB_OUTPUT:-}" ]]; then
		echo "scylla_version=$$resolved" >>"$$GITHUB_OUTPUT"
	fi

# KAFKA_VERSION: LATEST, PREVIOUS (newest patch of the previous MAJOR.MINOR line),
# <MAJOR>-LATEST (e.g. 7-LATEST), MAJOR.MINOR or a full version.
# get-version only supplies the list of releases: it cannot tell Debezium's 3.7.0.Final from
# 3.7.0.CR1, so the alias is picked here, the same way for every provider.
resolve-kafka-version: $(GET_VERSION)
	@set -euo pipefail
	if [[ "$(KAFKA_VERSION)" =~ $(FULL_VERSION_RE) ]]; then
		resolved="$(KAFKA_VERSION)"
	else
		case "$(KAFKA_PROVIDER)" in
			apache)
				releases=$$("$(GET_VERSION)" --source dockerhub-imagetag --repo apache/kafka -filters '^[0-9]+$$.^[0-9]+$$.^[0-9]+$$')
				;;
			confluent)
				# cp-kafka and cp-schema-registry are released under the same tags.
				releases=$$("$(GET_VERSION)" --source dockerhub-imagetag --repo confluentinc/cp-kafka-connect -filters '^[0-9]+$$.^[0-9]+$$.^[0-9]+$$')
				;;
			debezium)
				# The images are on quay.io, which get-version cannot read; the release tags match them.
				releases=$$("$(GET_VERSION)" --source github-tag --repo debezium/debezium --prefix v --out-no-prefix | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.Final$$' || true)
				;;
			*)
				echo "Unknown Kafka provider '$(KAFKA_PROVIDER)'; expected apache, confluent or debezium" >&2
				exit 1
				;;
		esac
		releases=$$(tr -d '"' <<<"$$releases" | sort -V)
		lines=$$(sed -E 's/^([0-9]+\.[0-9]+)\..*/\1/' <<<"$$releases" | uniq)
		latest_in() { awk -v p="$$1." 'index($$0, p) == 1' <<<"$$releases" | tail -n 1; }
		case "$(KAFKA_VERSION)" in
			LATEST)   resolved=$$(tail -n 1 <<<"$$releases") ;;
			PREVIOUS) resolved=$$(latest_in "$$(tail -n 2 <<<"$$lines" | head -n 1)") ;;
			*)
				if [[ "$(KAFKA_VERSION)" =~ ^([0-9]+)-LATEST$$ || "$(KAFKA_VERSION)" =~ ^([0-9]+\.[0-9]+)$$ ]]; then
					resolved=$$(latest_in "$${BASH_REMATCH[1]}")
				else
					echo "Unknown Kafka version '$(KAFKA_VERSION)'; expected LATEST, PREVIOUS, <MAJOR>-LATEST, MAJOR.MINOR or MAJOR.MINOR.PATCH" >&2
					exit 1
				fi
				;;
		esac
	fi
	if [[ ! "$$resolved" =~ $(FULL_VERSION_RE) ]]; then
		echo "Failed to resolve $(KAFKA_PROVIDER) '$(KAFKA_VERSION)', got '$$resolved'" >&2
		exit 1
	fi
	echo "Resolved $(KAFKA_PROVIDER) $(KAFKA_VERSION) to $$resolved"
	if [[ -n "$${GITHUB_OUTPUT:-}" ]]; then
		echo "kafka_version=$$resolved" >>"$$GITHUB_OUTPUT"
	fi
