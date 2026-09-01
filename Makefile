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
MAVEN_GPG_PASSPHRASE ?=
MAVEN_OPTS ?=

SONATYPE_TOKEN_USERNAME ?=
SONATYPE_TOKEN_PASSWORD ?=

RELEASE_SKIP_TESTS ?= false
RELEASE_TARGET_TAG ?=

# Set COVERAGE=true on any of the test-* targets to attach the JaCoCo agent to the forked test
# JVMs; `make coverage-report` then aggregates whatever execution data is on disk.
COVERAGE ?= false
COVERAGE_LOWER := $(shell printf '%s' '$(COVERAGE)' | tr '[:upper:]' '[:lower:]')
# An exported but empty COVERAGE means off: the `?=` above does not apply to a variable that is
# defined and empty, and aborting every unrelated target over it would be absurd.
ifeq ($(strip $(COVERAGE_LOWER)),)
COVERAGE_LOWER := false
endif
# A misspelled value is rejected rather than ignored: it used to produce a normal-looking run with
# no coverage in it, and in CI that silently drops one lane out of the aggregate.
ifeq ($(filter true 1 false 0,$(COVERAGE_LOWER)),)
$(error COVERAGE must be true or false, got '$(COVERAGE)')
endif
ifeq ($(filter true 1,$(COVERAGE_LOWER)),)
	MVN_COVERAGE =
else
	MVN_COVERAGE = -Pcoverage -Dcoverage.lane=$(COVERAGE_LANE)
endif
# Names this lane's execution data file, so that no two lanes ever write to one file. The default
# is set per target below; CI overrides it with the matrix entry its uploaded artifact is named
# after. That override has to be a make command-line variable, as in
# `make test-unit COVERAGE_LANE=unit-8`: a target-specific assignment beats the environment, so
# passing it as `env:` would quietly do nothing and put several lanes back on one file name.
COVERAGE_LANE = local
COVERAGE_REPORT_DIR := driver-coverage-report/target/site/jacoco-aggregate
# Must stay in step with driver-coverage-report's dependencies: jacoco:report-aggregate reads
# target/*.exec from those modules and nowhere else, so data anywhere else is not a report.
COVERAGE_EXEC_DIRS := driver-core/target driver-mapping/target driver-extras/target

ifeq (${CCM_CONFIG_DIR},)
	CCM_CONFIG_DIR = ~/.ccm
endif
CCM_CONFIG_DIR := $(shell readlink --canonicalize ${CCM_CONFIG_DIR})

export SCYLLA_EXT_OPTS
export SCYLLA_VERSION
export PATH := $(MAKEFILE_PATH)/bin:$(PATH)

.download-test-dependencies:
	$(MVNCMD) test -Dtest=TestThatDoesNotExists -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true || true

.download-verify-dependencies:
	$(MVNCMD) verify -DskipTests || true

.prepare-bin:
	@[[ -d "$(MAKEFILE_PATH)/bin" ]] || mkdir "$(MAKEFILE_PATH)/bin"

.prepare-get-version: .prepare-bin
	@if [[ ! -f "$(MAKEFILE_PATH)/bin/get-version" ]]; then
		echo "bin/get-version is not found, installing it"
		curl -sSLo /tmp/get-version.zip https://github.com/scylladb-actions/get-version/releases/download/v0.3.0/get-version_0.3.0_linux_amd64v3.zip
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

# JaCoCo appends to its execution data files by default, which is what lets a single lane
# accumulate coverage across several forks. The flip side is that data from an earlier run
# survives a recompile, and classes that changed in between are then reported as uncovered
# because their checksum no longer matches. Truncating before a run is the fix.
#
# Only this lane's file is removed, and only this lane ever writes it: another lane's results are
# never in the way, and never left to go stale behind this one's back. Expanded inside each test
# recipe rather than made a prerequisite of them, because make builds a prerequisite once per
# invocation: `make test-unit test-integration-scylla COVERAGE=true` would then truncate the
# first lane's file only, and let the second lane append to whatever it found.
CLEAN_COVERAGE_DATA = $(if $(MVN_COVERAGE),find . -name 'jacoco-$(COVERAGE_LANE).exec' -delete,:)

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

CASSANDRA_VERSION_FILE=/tmp/cassandra-version-${CASSANDRA_VERSION}.resolved
resolve-cassandra-version: .prepare-get-version
	@find "${CASSANDRA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1
	if [[ -f "${CASSANDRA_VERSION_FILE}" ]]; then
		echo "Resolved Cassandra ${CASSANDRA_VERSION} to $$(cat ${CASSANDRA_VERSION_FILE})"
		exit 0
	fi

	if [[ "${CASSANDRA_VERSION}" == "4-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 4.LAST.LAST" | tr -d '\"')
	elif [[ "${CASSANDRA_VERSION}" == "3-LATEST" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and 3.LAST.LAST" | tr -d '\"')
	elif [[ "${CASSANDRA_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$$ ]]; then
		CASSANDRA_VERSION_RESOLVED=${CASSANDRA_VERSION}
	elif [[ "${CASSANDRA_VERSION}" =~ ^[0-9]+\.[0-9]+$$ ]]; then
		# Complete a two-component version to its newest patch. See the comment in
		# resolve-scylla-version for why a partial version must not reach CCM.
		CASSANDRA_VERSION_RESOLVED=$$(get-version -source github-tag -repo apache/cassandra -prefix "cassandra-" -out-no-prefix -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and ${CASSANDRA_VERSION}.LAST" | tr -d '\"')
	elif echo "${CASSANDRA_VERSION}" | grep -qP '^[0-9]+\.[0-9]+'; then
		# Pre-release and other suffixed forms (4.0-alpha1, 5.0-beta1) already name one
		# exact build, so pass them through and skip the check below.
		CASSANDRA_VERSION_RESOLVED=${CASSANDRA_VERSION}
		CASSANDRA_VERSION_EXACT=1
	else
		echo "Unknown Cassandra version name '${CASSANDRA_VERSION}'"
		exit 1
	fi

	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "Failed to resolve Cassandra ${CASSANDRA_VERSION}"
		exit 1
	fi

	if [[ -z "$${CASSANDRA_VERSION_EXACT}" ]] && [[ ! "$${CASSANDRA_VERSION_RESOLVED}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$$ ]]; then
		echo "Resolved Cassandra version '$${CASSANDRA_VERSION_RESOLVED}' is not fully qualified, expected MAJOR.MINOR.PATCH"
		exit 1
	fi

	echo "Resolved Cassandra ${CASSANDRA_VERSION} to $${CASSANDRA_VERSION_RESOLVED}"
	if [[ -n "${GITHUB_OUTPUT}" ]]; then
		echo "value=$${CASSANDRA_VERSION_RESOLVED}" >>$${GITHUB_OUTPUT}
	fi
	echo "$${CASSANDRA_VERSION_RESOLVED}" >${CASSANDRA_VERSION_FILE}

SCYLLA_VERSION_FILE=/tmp/scylla-version-${SCYLLA_VERSION}.resolved
resolve-scylla-version: .prepare-get-version
	@find "${SCYLLA_VERSION_FILE}" -mtime +0 -delete 2>/dev/null 1>&1
	if [[ -f "${SCYLLA_VERSION_FILE}" ]]; then
		echo "Resolved ScyllaDB ${SCYLLA_VERSION} to $$(cat ${SCYLLA_VERSION_FILE})"
		exit 0
	fi

	if [[ "${SCYLLA_VERSION}" == "LTS-LATEST" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.1.LAST" | tr -d '\"')
	elif [[ "${SCYLLA_VERSION}" == "LTS-PRIOR" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST-1.1.LAST" | tr -d '\"')
		if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
			SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla-enterprise -filters "^[0-9]{4}$.^[0-9]+$.^[0-9]+$ and LAST-1.1.LAST" | tr -d '\"')
		fi
	elif [[ "${SCYLLA_VERSION}" == "LATEST" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST" | tr -d '\"')
	elif [[ "${SCYLLA_VERSION}" == "PRIOR" ]]; then
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]{4}$$.^[0-9]+$$.^[0-9]+$$ and LAST.LAST.LAST-1" | tr -d '\"')
	elif [[ "${SCYLLA_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$$ ]]; then
		SCYLLA_VERSION_RESOLVED=${SCYLLA_VERSION}
	elif [[ "${SCYLLA_VERSION}" =~ ^[0-9]+\.[0-9]+$$ ]]; then
		# A two-component version such as 2026.2 is accepted by CCM, but CCM then stores
		# the downloaded release under its full version while looking it up under the
		# partial one, so the lookup never hits the cache: every single 'ccm create'
		# re-queries S3 for the newest patch and the integration tests take several
		# times longer. Resolve it here, once, so the tests get a cache hit instead.
		SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and ${SCYLLA_VERSION}.LAST" | tr -d '\"')
		if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
			SCYLLA_VERSION_RESOLVED=$$(get-version --source dockerhub-imagetag --repo scylladb/scylla-enterprise -filters "^[0-9]+$$.^[0-9]+$$.^[0-9]+$$ and ${SCYLLA_VERSION}.LAST" | tr -d '\"')
		fi
	elif echo "${SCYLLA_VERSION}" | grep -qP '^[0-9]+\.[0-9]+'; then
		# Pre-release and other suffixed forms (2022.2.0-rc0, 5.0.rc3) already name one
		# exact build, so pass them through and skip the check below.
		SCYLLA_VERSION_RESOLVED=${SCYLLA_VERSION}
		SCYLLA_VERSION_EXACT=1
	else
		echo "Unknown ScyllaDB version name '${SCYLLA_VERSION}'"
		exit 1
	fi

	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		echo "Failed to resolve ScyllaDB '${SCYLLA_VERSION}'"
		exit 1
	fi

	if [[ -z "$${SCYLLA_VERSION_EXACT}" ]] && [[ ! "$${SCYLLA_VERSION_RESOLVED}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$$ ]]; then
		echo "Resolved ScyllaDB version '$${SCYLLA_VERSION_RESOLVED}' is not fully qualified, expected MAJOR.MINOR.PATCH"
		echo "A partial version makes every 'ccm create' query S3 for the newest patch,"
		echo "which makes the integration tests several times slower."
		exit 1
	fi

	echo "Resolved ScyllaDB ${SCYLLA_VERSION} to $${SCYLLA_VERSION_RESOLVED}"
	if [[ -n "${GITHUB_OUTPUT}" ]]; then
		echo "value=$${SCYLLA_VERSION_RESOLVED}" >>$${GITHUB_OUTPUT}
	fi
	echo "$${SCYLLA_VERSION_RESOLVED}" >${SCYLLA_VERSION_FILE}

download-cassandra: .prepare-scylla-ccm resolve-cassandra-version
	@if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		CASSANDRA_VERSION_RESOLVED=$$(cat '${CASSANDRA_VERSION_FILE}')
	fi
	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "Cassandra version ${CASSANDRA_VERSION} was not resolved"
		exit 1
	fi
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

checkout-one-commit-before:
	@if [[ "${RELEASE_TARGET_TAG}" == 3.* ]]; then
		echo "Checking out one commit before ${RELEASE_TARGET_TAG}"
		cp -f Makefile /tmp/tmp-Makefile
		git fetch --prune --unshallow || git fetch --prune || true
		git checkout ${RELEASE_TARGET_TAG}~1
		git tag -d ${RELEASE_TARGET_TAG}
		mv -f /tmp/tmp-Makefile ./Makefile
	fi

release-prepare: .require-release-prepare-env
	@if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	$(MVNCMD) release:prepare -DpushChanges=false

release: .require-release-env
	@if [[ "${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	mkdir /tmp/java-driver-release-logs/ 2>/dev/null || true
	$(MVNCMD) release:perform -Drelease.autopublish=true > >(tee /tmp/java-driver-release-logs/stdout.log) 2> >(tee /tmp/java-driver-release-logs/stderr.log)

release-dry-run: .require-release-env
	@if [[ -n "${RELEASE_SKIP_TESTS}" ]]; then
		export MAVEN_OPTS="${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	mkdir /tmp/java-driver-release-logs/ 2>/dev/null || true
	$(MVNCMD) release:perform > >(tee /tmp/java-driver-release-logs/stdout.log) 2> >(tee /tmp/java-driver-release-logs/stderr.log)

compile-all:
	mvn -B compile test-compile -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true

check:
	$(MVNCMD) verify -DskipTests

fix:
	$(MVNCMD) fmt:format

test-unit: COVERAGE_LANE = unit
test-unit:
	$(CLEAN_COVERAGE_DATA)
	$(MVNCMD) test $(MVN_COVERAGE) -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true

test-integration-scylla: COVERAGE_LANE = scylla-$(SCYLLA_VERSION)
test-integration-scylla: .prepare-scylla-ccm resolve-scylla-version .prepare-environment-update-aio-max-nr
	@if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		SCYLLA_VERSION_RESOLVED=`cat '${SCYLLA_VERSION_FILE}'`
	fi
	if [[ -z "$${SCYLLA_VERSION_RESOLVED}" ]]; then
		echo "ScyllaDB version ${SCYLLA_VERSION} was not resolved"
		exit 1
	fi
	$(CLEAN_COVERAGE_DATA)
	mvn -B verify -Pshort $(MVN_COVERAGE) -Dscylla.version=$${SCYLLA_VERSION_RESOLVED} -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true

test-integration-cassandra: COVERAGE_LANE = cassandra-$(CASSANDRA_VERSION)
test-integration-cassandra: .prepare-scylla-ccm resolve-cassandra-version
	@if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		CASSANDRA_VERSION_RESOLVED=`cat '${CASSANDRA_VERSION_FILE}'`
	fi
	if [[ -z "$${CASSANDRA_VERSION_RESOLVED}" ]]; then
		echo "Cassandra version ${CASSANDRA_VERSION} was not resolved"
		exit 1
	fi
	$(CLEAN_COVERAGE_DATA)
	mvn -B verify -Pshort $(MVN_COVERAGE) -Dcassandra.version=$${CASSANDRA_VERSION_RESOLVED} -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true

# Aggregates the execution data left behind by any COVERAGE=true test run into a single report.
# Tests are skipped here on purpose: this only reads what is already on disk, so the same target
# works for one local lane and for execution data collected from several CI jobs. Only test
# execution is skipped, not test compilation: -Dmaven.test.skip=true would also suppress
# driver-core's test-jar, which driver-mapping and driver-extras resolve at test scope, and
# nothing in this build or in CI installs that jar for them to fall back on.
#
# mvn is called directly rather than through MVNCMD, whose -X default would tee a debug log
# measured in hundreds of megabytes into the temp file this target then greps.
coverage-report:
	@set -eo pipefail
	exec_files=$$(find $(COVERAGE_EXEC_DIRS) -maxdepth 1 -name '*.exec' 2>/dev/null | sort || true)
	if [[ -z "$$exec_files" ]]; then
		echo 'No JaCoCo execution data found in $(COVERAGE_EXEC_DIRS).'
		echo "Run the tests with COVERAGE=true first, e.g. 'make test-unit COVERAGE=true'."
		exit 1
	fi
	rm -rf '${COVERAGE_REPORT_DIR}'
	maven_log=$$(mktemp)
	trap 'rm -f "$$maven_log"' EXIT
	mvn -B -ntp -Pcoverage-report -DskipTests verify -pl driver-coverage-report -am -Dfmt.skip=true -Dclirr.skip=true -Danimal.sniffer.skip=true 2>&1 | tee "$$maven_log"
	if [[ ! -f '${COVERAGE_REPORT_DIR}/jacoco.xml' ]]; then
		echo 'Maven produced no report at ${COVERAGE_REPORT_DIR}/jacoco.xml.'
		exit 1
	fi
	# The report's own LINE counter, not a sum over jacoco.csv: the CSV has a row per class, and
	# classes that share a source file share its lines, so adding the rows up counts those lines
	# once per class. On this tree that alone moved the figure by 46 lines. This is the number
	# JaCoCo puts in the HTML report, and it needs no assumption about column positions.
	summary=$$(python3 -c 'import sys, xml.etree.ElementTree as ET; c = next(x for x in ET.parse(sys.argv[1]).getroot().findall("counter") if x.get("type") == "LINE"); missed, covered = int(c.get("missed")), int(c.get("covered")); total = missed + covered; print(covered, total, "{:.2f}%".format(100.0 * covered / total) if total else "n/a")' '${COVERAGE_REPORT_DIR}/jacoco.xml')
	read -r covered total percentage <<< "$$summary"
	mismatched=$$(grep -c 'does not match' "$$maven_log" || true)
	# Which lanes went into the number is part of the number. A lane that was skipped or whose
	# upload failed lowers the percentage, and on its own a lower percentage reads as a regression.
	{
		echo "Line coverage: $$covered/$$total ($$percentage)"
		echo 'Aggregated from:'
		printf '%s\n' "$$exec_files" | sed 's/^/  /'
		if (( mismatched > 0 )); then
			echo "WARNING: $$mismatched classes were dropped because their execution data was recorded"
			echo "against differently compiled bytecode, so real coverage is higher than reported."
		fi
	} | tee -a "$${GITHUB_STEP_SUMMARY:-/dev/null}"
	echo 'HTML report: ${COVERAGE_REPORT_DIR}/index.html'
	if (( covered == 0 )); then
		echo 'Nothing is recorded as covered: the execution data does not match these classes.' >&2
		exit 1
	fi


clean-coverage:
	@find . -name 'jacoco*.exec' -delete
	rm -rf '${COVERAGE_REPORT_DIR}'

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
	rm -f release.properties
	for dir in examples mapper-processor mapper-runtime test-infra query-builder integration-tests bom; do
		rm -rf $$dir 2>/dev/null
	done
