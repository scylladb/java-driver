SHELL := bash
.ONESHELL:

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
SCYLLA_VERSION ?= LATEST
CASSANDRA_VERSION ?= 4-LATEST

CCM_CASSANDRA_REPO ?= github.com/apache/cassandra-ccm
CCM_CASSANDRA_VERSION ?= 3ef48de49e428e27653f5639f492a99851e2282d

CCM_SCYLLA_REPO ?= github.com/scylladb/scylla-ccm
CCM_SCYLLA_VERSION ?= master

SCYLLA_EXT_OPTS ?= --smp=2 --memory=4G
MVNCMD ?= mvn -B -X -ntp

GET_VERSION_VERSION ?= 0.4.3

MAVEN_GPG_PASSPHRASE ?=
SONATYPE_TOKEN_USERNAME ?=
SONATYPE_TOKEN_PASSWORD ?=

MAVEN_OPTS ?=

RELEASE_SKIP_TESTS ?=

ifeq (${CCM_CONFIG_DIR},)
	CCM_CONFIG_DIR = ~/.ccm
endif
CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})

export SCYLLA_EXT_OPTS
export SCYLLA_VERSION
export PATH := $(MAKEFILE_PATH)/bin:$(PATH)

.install-guava-shaded:
	$(MVNCMD) install -pl guava-shaded

.install-all-modules:
	$(MVNCMD) install -DskipTests -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true

.download-test-dependencies:
	$(MVNCMD) test -Dtest=TestThatDoesNotExists -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true || true

.download-verify-dependencies:
	$(MVNCMD) verify -DskipTests || true

.prepare-bin:
	@[[ -d "$(MAKEFILE_PATH)/bin" ]] || mkdir "$(MAKEFILE_PATH)/bin"

.prepare-get-version: .prepare-bin
	@if [[ ! -f "$(MAKEFILE_PATH)/bin/get-version" ]]; then
		echo "bin/get-version is not found, installing it"
		curl -sSLo /tmp/get-version.zip https://github.com/scylladb-actions/get-version/releases/download/v$(GET_VERSION_VERSION)/get-version_$(GET_VERSION_VERSION)_linux_amd64v3.zip
		unzip /tmp/get-version.zip get-version -d "$(MAKEFILE_PATH)/bin" >/dev/null
	fi

.prepare-scylla-ccm:
	@ccm --help 2>/dev/null 1>&2
	if [[ $$? -lt 127 ]] \
		&& grep SCYLLA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 \
		&& grep ${CCM_SCYLLA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev/null 1>&2; then
		echo "ScyllaDB CCM ${CCM_SCYLLA_VERSION} is already installed"
	else \
  		$(MAKE) install-scylla-ccm
	fi

.prepare-cassandra-ccm:
	@ccm --help 2>/dev/null 1>&2
	if [[ $$? -lt 127 ]] \
		&& grep CASSANDRA ${CCM_CONFIG_DIR}/ccm-type 2>/dev/null 1>&2 \
		&& grep ${CCM_CASSANDRA_VERSION} ${CCM_CONFIG_DIR}/ccm-version 2>/dev/null 1>&2; then
		echo "Cassandra CCM ${CCM_CASSANDRA_VERSION} is already installed"
	else \
  		$(MAKE) install-cassandra-ccm
	fi

.prepare-environment-update-aio-max-nr:
	@if (( $$(< /proc/sys/fs/aio-max-nr) < 2097152 )); then
		echo 2097152 | sudo tee /proc/sys/fs/aio-max-nr >/dev/null
	fi

install-cassandra-ccm:
	@echo "Install CCM ${CCM_CASSANDRA_VERSION}"
	pip install "git+https://${CCM_CASSANDRA_REPO}.git@${CCM_CASSANDRA_VERSION}"
	mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	echo CASSANDRA > ${CCM_CONFIG_DIR}/ccm-type
	echo ${CCM_CASSANDRA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

install-scylla-ccm:
	@echo "Installing ScyllaDB CCM ${CCM_SCYLLA_VERSION}"
	pip install "git+https://${CCM_SCYLLA_REPO}.git@${CCM_SCYLLA_VERSION}"
	mkdir ${CCM_CONFIG_DIR} 2>/dev/null || true
	echo SCYLLA > ${CCM_CONFIG_DIR}/ccm-type
	echo ${CCM_SCYLLA_VERSION} > ${CCM_CONFIG_DIR}/ccm-version

download-all-dependencies: compile-all .download-test-dependencies .download-verify-dependencies

# A server version is usable only when it names one build: MAJOR.MINOR.PATCH, or an exact
# pre-release build (2022.2.0-rc0, 5.0.rc3, 4.0-alpha1, 2022.1.3-dev-0.20220922.539a55e35).
# A pre-release label must start with a letter and carry a numeric discriminator, so a bare
# selector such as 6.2.0~rc or 6.2.0-dev is not one: CCM resolves it to whichever build is
# newest today. Anything less - a bare MAJOR.MINOR above all - makes every 'ccm create'
# re-query S3 for the newest patch instead of reusing the release it already installed.
# Used by the resolvers below and by every target that hands a version to CCM, so that one
# grammar decides all of them.
SERVER_VERSION_RE = ^[0-9]+\.[0-9]+\.[0-9]+$$|^[0-9]+\.[0-9]+(\.[0-9]+)?[-~.][A-Za-z][A-Za-z._~-]*[0-9][A-Za-z0-9._~-]*$$

# $(1) = name of the shell var to hold the cached value (e.g. CASSANDRA_VERSION_CACHED)
# $(2) = cache file path (e.g. ${CASSANDRA_VERSION_FILE})
define LOAD_CACHED_VERSION
	version_re='$(SERVER_VERSION_RE)'

	# The cache is shared and outlives a Makefile change, so an entry written before this
	# check existed can hold a version that is not fully qualified. Drop any such entry
	# and resolve again, rather than serve it and bypass the check below.
	$(1)=
	if [[ -f "$(2)" ]]; then
		$(1)=$$(cat "$(2)")
		if [[ ! "$$$(1)" =~ $$version_re ]]; then
			rm -f "$(2)"
			$(1)=
		fi
	fi
endef

# $(1) = name of the shell var holding the resolved version
# $(2) = human label for the error message (e.g. Cassandra, ScyllaDB)
define REQUIRE_FULLY_QUALIFIED_VERSION
	version_re='$(SERVER_VERSION_RE)'
	if [[ ! "$$$(1)" =~ $$version_re ]]; then
		echo "$(2) version '$$$(1)' does not name one build, expected MAJOR.MINOR.PATCH or an"
		echo "exact pre-release build such as 2022.2.0-rc0 - a bare '-rc'/'-dev' selector is not one"
		exit 1
	fi
endef

CASSANDRA_VERSION_FILE=/tmp/cassandra-version-${CASSANDRA_VERSION}.resolved
resolve-cassandra-version: .prepare-get-version
	@find "${CASSANDRA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1
	# Set here as well as in the macros below, which set it for their own use: the
	# pass-through branch matches against it directly, and bash treats an unset
	# version_re as the empty regex, which every string matches.
	version_re='$(SERVER_VERSION_RE)'
	$(call LOAD_CACHED_VERSION,CASSANDRA_VERSION_CACHED,${CASSANDRA_VERSION_FILE})

	if [[ -n "$${CASSANDRA_VERSION_CACHED}" ]]; then
		CASSANDRA_VERSION_RESOLVED=$${CASSANDRA_VERSION_CACHED}
	elif [[ "${CASSANDRA_VERSION}" == "4-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 4.LAST.LAST" | tr -d '\"')
	elif [[ "${CASSANDRA_VERSION}" == "3-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 3.LAST.LAST" | tr -d '\"')
	elif [[ "${CASSANDRA_VERSION}" =~ $$version_re ]]; then
		# Already fully qualified, or a suffixed form (4.0-alpha1, 5.0-beta1) naming one exact
		# build. Pass it through untouched, with no lookup.
		CASSANDRA_VERSION_RESOLVED=${CASSANDRA_VERSION}
	elif [[ "${CASSANDRA_VERSION}" =~ ^[0-9]+\.[0-9]+$$ ]]; then
		# Complete a two-component version to its newest patch. See the comment in
		# resolve-scylla-version for why a partial version must not reach CCM.
		CASSANDRA_VERSION_RESOLVED=$$(get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and ${CASSANDRA_VERSION}.LAST" | tr -d '\"')
	else
		echo "Unknown Cassandra version name '${CASSANDRA_VERSION}'"
		echo "Expected 3-LATEST, 4-LATEST, MAJOR.MINOR.PATCH, MAJOR.MINOR, or an exact pre-release"
		echo "build such as 4.0-alpha1 - a bare '-rc'/'-dev' selector is not one"
		exit 1
	fi

	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "Failed to resolve Cassandra ${CASSANDRA_VERSION}"
		exit 1
	fi

	$(call REQUIRE_FULLY_QUALIFIED_VERSION,CASSANDRA_VERSION_RESOLVED,Cassandra)

	echo "Resolved Cassandra ${CASSANDRA_VERSION} to $${CASSANDRA_VERSION_RESOLVED}"
	if [[ -n "${GITHUB_OUTPUT}" ]]; then
		echo "value=$${CASSANDRA_VERSION_RESOLVED}" >>$${GITHUB_OUTPUT}
	fi
	# Only a fresh resolve writes the cache. Rewriting the file on a hit would refresh
	# the mtime that the 'find -mtime +0' above expires on, so an alias used at least
	# once a day would pin to the patch it first resolved to and never age out.
	if [[ -z "$${CASSANDRA_VERSION_CACHED}" ]]; then
		echo "$${CASSANDRA_VERSION_RESOLVED}" >${CASSANDRA_VERSION_FILE}
	fi

SCYLLA_VERSION_FILE=/tmp/scylla-version-${SCYLLA_VERSION}.resolved
resolve-scylla-version: .prepare-get-version
	@find "${SCYLLA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1
	# Set here as well as in the macros below, which set it for their own use: the
	# pass-through branch matches against it directly, and bash treats an unset
	# version_re as the empty regex, which every string matches.
	version_re='$(SERVER_VERSION_RE)'
	$(call LOAD_CACHED_VERSION,SCYLLA_VERSION_CACHED,${SCYLLA_VERSION_FILE})

	if [[ -n "$${SCYLLA_VERSION_CACHED}" ]]; then
		SCYLLA_VERSION_RESOLVED=$${SCYLLA_VERSION_CACHED}
	elif [[ "${SCYLLA_VERSION}" == "LTS-LATEST" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.1.LAST" | tr -d '\"')
	elif [[ "${SCYLLA_VERSION}" == "LTS-PRIOR" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST-1.1.LAST" | tr -d '\"')
		if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
			SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla-enterprise -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST-1.1.LAST" | tr -d '\"')
		fi
	elif [[ "${SCYLLA_VERSION}" == "LATEST" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST" | tr -d '\"')
	elif [[ "${SCYLLA_VERSION}" == "PRIOR" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST-1" | tr -d '\"')
	elif [[ "${SCYLLA_VERSION}" =~ $$version_re ]]; then
		# Already fully qualified, or a suffixed form (2022.2.0-rc0, 5.0.rc3) naming one exact
		# build. Pass it through untouched, with no lookup.
		SCYLLA_VERSION_RESOLVED=${SCYLLA_VERSION}
	elif [[ "${SCYLLA_VERSION}" =~ ^[0-9]+\.[0-9]+$$ ]]; then
		# A two-component version such as 2026.2 is accepted by CCM, but CCM then stores
		# the downloaded release under its full version while looking it up under the
		# partial one, so the lookup never hits its own entry: every single 'ccm create'
		# re-queries S3 for the newest patch. Resolve it here, once, so that the cache
		# entry CCM writes is the one it later looks up.
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and ${SCYLLA_VERSION}.LAST" | tr -d '\"')
		# Releases before 2025.1 are in the scylla-enterprise repo, and only ever under a
		# four-digit year, so an OSS line (6.2, 5.4) must not pay for a second lookup.
		if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]] && [[ "${SCYLLA_VERSION}" =~ ^[0-9]{4}\. ]]; then
			SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla-enterprise -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and ${SCYLLA_VERSION}.LAST" | tr -d '\"')
		fi
	else
		echo "Unknown ScyllaDB version name '${SCYLLA_VERSION}'"
		echo "Expected LATEST, PRIOR, LTS-LATEST, LTS-PRIOR, MAJOR.MINOR.PATCH, MAJOR.MINOR, or an"
		echo "exact pre-release build such as 2022.2.0-rc0 - a bare '-rc'/'-dev' selector is not one"
		exit 1
	fi

	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		echo "Failed to resolve ScyllaDB '${SCYLLA_VERSION}'"
		exit 1
	fi

	$(call REQUIRE_FULLY_QUALIFIED_VERSION,SCYLLA_VERSION_RESOLVED,ScyllaDB)

	echo "Resolved ScyllaDB ${SCYLLA_VERSION} to $${SCYLLA_VERSION_RESOLVED}"
	if [[ -n "${GITHUB_OUTPUT}" ]]; then
		echo "value=$${SCYLLA_VERSION_RESOLVED}" >>$${GITHUB_OUTPUT}
	fi
	# Only a fresh resolve writes the cache. Rewriting the file on a hit would refresh
	# the mtime that the 'find -mtime +0' above expires on, so an alias used at least
	# once a day would pin to the patch it first resolved to and never age out.
	if [[ -z "$${SCYLLA_VERSION_CACHED}" ]]; then
		echo "$${SCYLLA_VERSION_RESOLVED}" >${SCYLLA_VERSION_FILE}
	fi

checkout-one-commit-before:
	@if [[ "${RELEASE_TARGET_TAG}" == 4.* ]]; then
		echo "Checking out one commit before ${RELEASE_TARGET_TAG}"
		git fetch --prune --unshallow || git fetch --prune || true
		git checkout ${RELEASE_TARGET_TAG}~1
		git tag -d ${RELEASE_TARGET_TAG}
	fi

download-cassandra: .prepare-scylla-ccm resolve-cassandra-version
	@if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(cat '${CASSANDRA_VERSION_FILE}')
	fi
	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "Cassandra version ${CASSANDRA_VERSION} was not resolved"
		exit 1
	fi
	# Check it here too, where the version is handed to CCM: this value can arrive from
	# the environment, which is how CI passes it, so the resolver never saw it.
	$(call REQUIRE_FULLY_QUALIFIED_VERSION,CASSANDRA_VERSION_RESOLVED,Cassandra)
	rm -rf /tmp/download.ccm || true
	mkdir /tmp/download.ccm || true
	ccm create ccm_1 -i 127.0.254. -n 1:0 -v "$${CASSANDRA_VERSION_RESOLVED}" --config-dir=/tmp/download.ccm
	rm -rf /tmp/download.ccm

download-scylla: .prepare-scylla-ccm resolve-scylla-version
	@if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(cat '${SCYLLA_VERSION_FILE}')
	fi
	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		echo "ScyllaDB version ${SCYLLA_VERSION} was not resolved"
		exit 1
	fi
	# Check it here too, where the version is handed to CCM: this value can arrive from
	# the environment, which is how CI passes it, so the resolver never saw it.
	$(call REQUIRE_FULLY_QUALIFIED_VERSION,SCYLLA_VERSION_RESOLVED,ScyllaDB)
	rm -rf /tmp/download.ccm || true
	mkdir /tmp/download.ccm || true
	ccm create ccm_1 -i 127.0.254. -n 1:0 -v "$${SCYLLA_VERSION_RESOLVED}" --scylla --config-dir=/tmp/download.ccm
	rm -rf /tmp/download.ccm

.require-release-prepare-env:
	@if [[ -z "${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty"
		exit 1
	fi

.require-release-env:
	@if [[ -z "${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty"
		exit 1
	fi
	if [[ -z "${SONATYPE_TOKEN_USERNAME}" ]]; then
		echo "SONATYPE_TOKEN_USERNAME is empty"
		exit 1
	fi
	if [[ -z "${SONATYPE_TOKEN_PASSWORD}" ]]; then
		echo "SONATYPE_TOKEN_PASSWORD is empty"
		exit 1
	fi

release-prepare: .require-release-prepare-env
	@if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	# maven-release-plugin rewrites pom.xml via its own XML serializer (MavenXpp3Writer)
	# which does not conform to xml-format-maven-plugin rules (e.g. expands <foo/> to <foo />).
	# The check is skipped during release:prepare to avoid build failure, but we immediately
	# re-format all pom.xml files and amend the resulting SNAPSHOT commit so that downstream
	# branches and PRs do not inherit the formatting corruption in their CI merge commits.
	$(MVNCMD) release:prepare -DpushChanges=false -Dxml-format.skip=true
	$(MVNCMD) xml-format:xml-format -Dxml-format.skip=false
	git diff --name-only | grep 'pom\.xml' | xargs --no-run-if-empty git add
	git diff --cached --quiet || git commit --amend --no-edit

release: .require-release-env
	@if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	mkdir /tmp/java-driver-release-logs/ 2>/dev/null || true
	$(MVNCMD) release:perform -Drelease.autopublish=true > >(tee /tmp/java-driver-release-logs/stdout.log) 2> >(tee /tmp/java-driver-release-logs/stderr.log)

release-dry-run: .require-release-env
	@if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	mkdir /tmp/java-driver-release-logs/ 2>/dev/null || true
	$(MVNCMD) release:perform > >(tee /tmp/java-driver-release-logs/stdout.log) 2> >(tee /tmp/java-driver-release-logs/stderr.log)

compile-all: .install-guava-shaded
	mvn -B compile test-compile -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true

check:
	$(MVNCMD) verify -DskipTests

fix:
	$(MVNCMD) fmt:format xml-format:xml-format

test-unit: .install-guava-shaded
	$(MVNCMD) test -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true

test-integration-scylla: .install-all-modules .prepare-scylla-ccm resolve-scylla-version .prepare-environment-update-aio-max-nr
	@if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		SCYLLA_VERSION_RESOLVED=`cat '${SCYLLA_VERSION_FILE}'`
	fi
	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		echo "ScyllaDB version ${SCYLLA_VERSION} was not resolved"
		exit 1
	fi
	# Check it here too, where the version is handed to CCM: this value can arrive from
	# the environment, which is how CI passes it, so the resolver never saw it.
	$(call REQUIRE_FULLY_QUALIFIED_VERSION,SCYLLA_VERSION_RESOLVED,ScyllaDB)
	mvn -B -e verify -pl integration-tests -Dccm.version=$${SCYLLA_VERSION_RESOLVED} -Dccm.distribution=scylla -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true $(MAVEN_EXTRA_ARGS)

test-integration-cassandra: .install-all-modules .prepare-scylla-ccm resolve-cassandra-version
	@if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		CASSANDRA_VERSION_RESOLVED=`cat '${CASSANDRA_VERSION_FILE}'`
	fi
	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "Cassandra version ${CASSANDRA_VERSION} was not resolved"
		exit 1
	fi
	# Check it here too, where the version is handed to CCM: this value can arrive from
	# the environment, which is how CI passes it, so the resolver never saw it.
	$(call REQUIRE_FULLY_QUALIFIED_VERSION,CASSANDRA_VERSION_RESOLVED,Cassandra)
	mvn -B -e verify -pl integration-tests -Dccm.version=$${CASSANDRA_VERSION_RESOLVED} -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true $(MAVEN_EXTRA_ARGS)

check-no-compile-warnings:
	@$(MAKE) compile-all | grep WARNING >/tmp/all-compile-warnings.log || true
	if [ -s /tmp/all-compile-warnings.log ]; then
		echo "Found warnings in while compiling code:"
		cat /tmp/all-compile-warnings.log
		exit 1
	fi

clean:
	@mvn clean
	find -name 'pom.xml.releaseBackup' -delete
	find -name 'pom.xml.tag' -delete
	find -name 'pom.xml.next' -delete
	find -name 'target' -exec rm -rf {} +
	find -name 'dependency-reduced-pom.xml' -exec rm -f {} +
	rm -f release.properties 2>/dev/null
	for dir in driver-core driver-examples driver-extras driver-mapping driver-tests driver-dist testing; do
		rm -rf $$dir 2>/dev/null
	done
