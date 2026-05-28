RELEASE_TAG := $(shell git describe --tags --abbrev=0)
RELEASE_VERSION := $(RELEASE_TAG:v%=%)
ARTIFACT_ID := scylla-cdc-source-connector
CHECKOUT_TARGET := target/checkout/target

FAT_JAR := $(CHECKOUT_TARGET)/fat-jar/$(ARTIFACT_ID)-$(RELEASE_VERSION)-jar-with-dependencies.jar
CONNECT_ZIP := $(CHECKOUT_TARGET)/components/packages/scylladb-$(ARTIFACT_ID)-$(RELEASE_VERSION).zip

.PHONY: github-release

github-release:
	if gh release view "$(RELEASE_TAG)" > /dev/null 2>&1; then \
		gh release upload "$(RELEASE_TAG)" "$(FAT_JAR)" "$(CONNECT_ZIP)" --clobber; \
	else \
		gh release create "$(RELEASE_TAG)" \
			"$(FAT_JAR)" \
			"$(CONNECT_ZIP)" \
			--title "Release $(RELEASE_VERSION)" \
			--generate-notes; \
	fi
