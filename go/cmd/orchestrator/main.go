/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/math"
	"github.com/outbrain/orchestrator/go/app"
	"github.com/outbrain/orchestrator/go/config"
	"github.com/outbrain/orchestrator/go/inst"
)

var AppVersion string

const prompt string = `
orchestrator [-c command] [-i instance] [-d destination] [--verbose|--debug] [... cli ] | http

Cheatsheet:
    Run orchestrator in HTTP mode:

        orchestrator --debug http

    See all possible commands:

        orchestrator -c help

    Usage for most commands:
        orchestrator -c <command> [-i <instance.fqdn>] [-d <destination.fqdn>] [--verbose|--debug]

    -i (instance):
        instance on which to operate, in "hostname" or "hostname:port" format.
        Default port is 3306 (or DefaultInstancePort in config)
        For some commands this argument can be ommitted altogether, and the
        value is implicitly the local hostname.
    -d (Destination)
        destination instance (used when moving replicas around or when failing over)
    -s (Sibling/Subinstance/deStination) - synonym to "-d"

    -c (command):
        Listed below are all available commands; all of which apply for CLI execution (ignored by HTTP mode).
        Different flags are required for different commands; see specific documentation per commmand.

    Topology refactoring, generic aka "smart" commands
        These operations let orchestrator pick the best course of action for relocating subordinates. It may choose to use
        standard binlog file:pos math, GTID, Pseudo-GTID, or take advantage of binlog servers, or combine two or more
        methods in a multi-step operation.
        In case a of a multi-step operation, failure may result in subordinates only moving halfway to destination point. Nonetheless
        they will be in a valid position.

        relocate
            Relocate a subordinate beneath another (destination) instance. The choice of destination is almost arbitrary;
            it must not be a child/descendant of the instance, but otherwise it can be anywhere, and can be a normal subordinate
            or a binlog server. Orchestrator will choose the best course of action to relocate the subordinate.
            No action taken when destination instance cannot act as main (e.g. has no binary logs, is of incompatible version, incompatible binlog format etc.)
            Examples:

            orchestrator -c relocate -i subordinate.to.relocate.com -d instance.that.becomes.its.main

            orchestrator -c relocate -d destination.instance.that.becomes.its.main
                -i not given, implicitly assumed local hostname

            (this command was previously named "relocate-below")

        relocate-subordinates
            Relocates all or part of the subordinates of a given instance under another (destination) instance. This is
            typically much faster than relocating subordinates one by one.
            Orchestrator chooses the best course of action to relocation the subordinates. It may choose a multi-step operations.
            Some subordinates may succeed and some may fail the operation.
            The instance (subordinates' main) itself may be crashed or inaccessible. It is not contacted throughout the operation.
            Examples:

            orchestrator -c relocate-subordinates -i instance.whose.subordinates.will.relocate -d instance.that.becomes.their.main

            orchestrator -c relocate-subordinates -i instance.whose.subordinates.will.relocate -d instance.that.becomes.their.main --pattern=regexp.filter
                only apply to those instances that match given regex

    Topology refactoring using classic MySQL replication commands
        (ie STOP SLAVE; START SLAVE UNTIL; CHANGE MASTER TO; ...)
        These commands require connected topology: subordinates that are up and running; a lagging, stopped or
        failed subordinate will disable use of most these commands. At least one, and typically two or more subordinates
        will be stopped for a short time during these operations.

        move-up
            Move a subordinate one level up the topology; makes it replicate from its grandparent and become sibling of
            its parent. It is OK if the instance's main is not replicating. Examples:

            orchestrator -c move-up -i subordinate.to.move.up.com:3306

            orchestrator -c move-up
                -i not given, implicitly assumed local hostname

        move-up-subordinates
            Moves subordinates of the given instance one level up the topology, making them siblings of given instance.
            This is a (faster) shortcut to executing move-up on all subordinates of given instance.
            Examples:

            orchestrator -c move-up-subordinates -i subordinate.whose.subsubordinates.will.move.up.com[:3306]

            orchestrator -c move-up-subordinates -i subordinate.whose.subsubordinates.will.move.up.com[:3306] --pattern=regexp.filter
                only apply to those instances that match given regex

        move-below
            Moves a subordinate beneath its sibling. Both subordinates must be actively replicating from same main.
            The sibling will become instance's main. No action taken when sibling cannot act as main
            (e.g. has no binary logs, is of incompatible version, incompatible binlog format etc.)
            Example:

            orchestrator -c move-below -i subordinate.to.move.com -d sibling.subordinate.under.which.to.move.com

            orchestrator -c move-below -d sibling.subordinate.under.which.to.move.com
                -i not given, implicitly assumed local hostname

        move-equivalent
            Moves a subordinate beneath another server, based on previously recorded "equivalence coordinates". Such coordinates
            are obtained whenever orchestrator issues a CHANGE MASTER TO. The "before" and "after" mains coordinates are
            persisted. In such cases where the newly relocated subordinate is unable to replicate (e.g. firewall issues) it is then
            easy to revert the relocation via "move-equivalent".
            The command works if and only if orchestrator has an exact mapping between the subordinate's current replication coordinates
            and some other coordinates.
            Example:

            orchestrator -c move-equivalent -i subordinate.to.revert.main.position.com -d main.to.move.to.com

        ensubordinate-siblings
            Turn all siblings of a subordinate into its sub-subordinates. No action taken for siblings that cannot become
            subordinates of given instance (e.g. incompatible versions, binlog format etc.). This is a (faster) shortcut
            to executing move-below for all siblings of the given instance. Example:

            orchestrator -c ensubordinate-siblings -i subordinate.whose.siblings.will.move.below.com

        ensubordinate-main
            Turn an instance into a main of its own main; essentially switch the two. Subordinates of each of the two
            involved instances are unaffected, and continue to replicate as they were.
            The instance's main must itself be a subordinate. It does not necessarily have to be actively replicating.

            orchestrator -c ensubordinate-main -i subordinate.that.will.switch.places.with.its.main.com

        repoint
            Make the given instance replicate from another instance without changing the binglog coordinates. There
            are little sanity checks to this and this is a risky operation. Use cases are: a rename of the main's
            host, a corruption in relay-logs, move from beneath MaxScale & Binlog-server. Examples:

            orchestrator -c repoint -i subordinate.to.operate.on.com -d new.main.com

            orchestrator -c repoint -i subordinate.to.operate.on.com
                The above will repoint the subordinate back to its existing main without change

            orchestrator -c repoint
                -i not given, implicitly assumed local hostname

        repoint-subordinates
            Repoint all subordinates of given instance to replicate back from the instance. This is a convenience method
            which implies a one-by-one "repoint" command on each subordinate.

            orchestrator -c repoint-subordinates -i instance.whose.subordinates.will.be.repointed.com

            orchestrator -c repoint-subordinates
                -i not given, implicitly assumed local hostname

        make-co-main
            Create a main-main replication. Given instance is a subordinate which replicates directly from a main.
            The main is then turned to be a subordinate of the instance. The main is expected to not be a subordinate.
            The read_only property of the slve is unaffected by this operation. Examples:

            orchestrator -c make-co-main -i subordinate.to.turn.into.co.main.com

            orchestrator -c make-co-main
                -i not given, implicitly assumed local hostname

        get-candidate-subordinate
            Information command suggesting the most up-to-date subordinate of a given instance, which can be promoted
            as local main to its siblings. If replication is up and running, this command merely gives an
            estimate, since subordinates advance and progress continuously in different pace. If all subordinates of given
            instance have broken replication (e.g. because given instance is dead), then this command provides
            with a definitve candidate, which could act as a replace main. See also regroup-subordinates. Example:

            orchestrator -c get-candidate-subordinate -i instance.with.subordinates.one.of.which.may.be.candidate.com

        regroup-subordinates-bls
            Given an instance that has Binlog Servers for subordinates, promote one such Binlog Server over its other
            Binlog Server siblings.

            Example:

            orchestrator -c regroup-subordinates-bls -i instance.with.binlog.server.subordinates.com

            --debug is your friend.


    Topology refactoring using GTID
        These operations only work if GTID (either Oracle or MariaDB variants) is enabled on your servers.

        move-gtid
            Move a subordinate beneath another (destination) instance. Orchestrator will reject the operation if GTID is
            not enabled on the subordinate, or is not supported by the would-be main.
            You may try and move the subordinate under any other instance; there are no constraints on the family ties the
            two may have, though you should be careful as not to try and replicate from a descendant (making an
            impossible loop).
            Examples:

            orchestrator -c move-gtid -i subordinate.to.move.com -d instance.that.becomes.its.main

            orchestrator -c match -d destination.instance.that.becomes.its.main
                -i not given, implicitly assumed local hostname

        move-subordinates-gtid
            Moves all subordinates of a given instance under another (destination) instance using GTID. This is a (faster)
            shortcut to moving each subordinate via "move-gtid".
            Orchestrator will only move those subordinates configured with GTID (either Oracle or MariaDB variants) and under the
            condition the would-be main supports GTID.
            Examples:

            orchestrator -c move-subordinates-gtid -i instance.whose.subordinates.will.relocate -d instance.that.becomes.their.main

            orchestrator -c move-subordinates-gtid -i instance.whose.subordinates.will.relocate -d instance.that.becomes.their.main --pattern=regexp.filter
                only apply to those instances that match given regex

        regroup-subordinates-gtid
            Given an instance (possibly a crashed one; it is never being accessed), pick one of its subordinate and make it
            local main of its siblings, using GTID. The rules are similar to those in the "regroup-subordinates" command.
            Example:

            orchestrator -c regroup-subordinates-gtid -i instance.with.gtid.and.subordinates.one.of.which.will.turn.local.main.if.possible

            --debug is your friend.


    Topology refactoring using Pseudo-GTID
        These operations require that the topology's main is periodically injected with pseudo-GTID,
        and that the PseudoGTIDPattern configuration is setup accordingly. Also consider setting
        DetectPseudoGTIDQuery.
        Operations via Pseudo-GTID are typically slower, since they involve scanning of binary/relay logs.
        They impose less constraints on topology locations and affect less servers. Only servers that
        are being relocateed have their replication stopped. Their mains or destinations are unaffected.

        match
            Matches a subordinate beneath another (destination) instance. The choice of destination is almost arbitrary;
            it must not be a child/descendant of the instance. But otherwise they don't have to be direct siblings,
            and in fact (if you know what you're doing), they don't actually have to belong to the same topology.
            The operation expects the relocated instance to be "behind" the destination instance. It only finds out
            whether this is the case by the end; the operation is cancelled in the event this is not the case.
            No action taken when destination instance cannot act as main (e.g. has no binary logs, is of incompatible version, incompatible binlog format etc.)
            Examples:

            orchestrator -c match -i subordinate.to.relocate.com -d instance.that.becomes.its.main

            orchestrator -c match -d destination.instance.that.becomes.its.main
                -i not given, implicitly assumed local hostname

            (this command was previously named "match-below")

        match-subordinates
            Matches all subordinates of a given instance under another (destination) instance. This is a (faster) shortcut
            to matching said subordinates one by one under the destination instance. In fact, this bulk operation is highly
            optimized and can execute in orders of magnitue faster, depeding on the nu,ber of subordinates involved and their
            respective position behind the instance (the more subordinates, the more savings).
            The instance itself may be crashed or inaccessible. It is not contacted throughout the operation. Examples:

            orchestrator -c match-subordinates -i instance.whose.subordinates.will.relocate -d instance.that.becomes.their.main

            orchestrator -c match-subordinates -i instance.whose.subordinates.will.relocate -d instance.that.becomes.their.main --pattern=regexp.filter
                only apply to those instances that match given regex

            (this command was previously named "multi-match-subordinates")

        match-up
            Transport the subordinate one level up the hierarchy, making it child of its grandparent. This is
            similar in essence to move-up, only based on Pseudo-GTID. The main of the given instance
            does not need to be alive or connected (and could in fact be crashed). It is never contacted.
            Grandparent instance must be alive and accessible.
            Examples:

            orchestrator -c match-up -i subordinate.to.match.up.com:3306

            orchestrator -c match-up
                -i not given, implicitly assumed local hostname

        match-up-subordinates
            Matches subordinates of the given instance one level up the topology, making them siblings of given instance.
            This is a (faster) shortcut to executing match-up on all subordinates of given instance. The instance need
            not be alive / accessib;e / functional. It can be crashed.
            Example:

            orchestrator -c match-up-subordinates -i subordinate.whose.subsubordinates.will.match.up.com

            orchestrator -c match-up-subordinates -i subordinate.whose.subsubordinates.will.match.up.com[:3306] --pattern=regexp.filter
                only apply to those instances that match given regex

        rematch
            Reconnect a subordinate onto its main, via PSeudo-GTID. The use case for this operation is a non-crash-safe
            replication configuration (e.g. MySQL 5.5) with sync_binlog=1 and log_subordinate_updates. This operation
            implies crash-safe-replication and makes it possible for the subordinate to reconnect. Example:

            orchestrator -c rematch -i subordinate.to.rematch.under.its.main

        regroup-subordinates
            Given an instance (possibly a crashed one; it is never being accessed), pick one of its subordinate and make it
            local main of its siblings, using Pseudo-GTID. It is uncertain that there *is* a subordinate that will be able to
            become main to all its siblings. But if there is one, orchestrator will pick such one. There are many
            constraints, most notably the replication positions of all subordinates, whether they use log_subordinate_updates, and
            otherwise version compatabilities etc.
            As many subordinates that can be regrouped under promoted slves are operated on. The rest are untouched.
            This command is useful in the event of a crash. For example, in the event that a main dies, this operation
            can promote a candidate replacement and set up the remaining topology to correctly replicate from that
            replacement subordinate. Example:

            orchestrator -c regroup-subordinates -i instance.with.subordinates.one.of.which.will.turn.local.main.if.possible

            --debug is your friend.

    General replication commands
        These commands issue various statements that relate to replication.

        enable-gtid
            If possible, enable GTID replication. This works on Oracle (>= 5.6, gtid-mode=1) and MariaDB (>= 10.0).
            Replication is stopped for a short duration so as to reconfigure as GTID. In case of error replication remains
            stopped. Example:

            orchestrator -c enable-gtid -i subordinate.compatible.with.gtid.com

        disable-gtid
            Assuming subordinate replicates via GTID, disable GTID replication and resume standard file:pos replication. Example:

            orchestrator -c disable-gtid -i subordinate.replicating.via.gtid.com

        reset-main-gtid-remove-own-uuid
            Assuming GTID is enabled, Reset main on instance, remove GTID entries generated by the instance.
            This operation is only allowed on Oracle-GTID enabled servers that have no subordinates.
            Is is used for cleaning up the GTID mess incurred by mistakenly issuing queries on the subordinate (even such
            queries as "FLUSH ENGINE LOGS" that happen to write to binary logs). Example:

            orchestrator -c reset-main-gtid-remove-own-uuid -i subordinate.running.with.gtid.com

        stop-subordinate
            Issues a STOP SLAVE; command. Example:

            orchestrator -c stop-subordinate -i subordinate.to.be.stopped.com

        start-subordinate
            Issues a START SLAVE; command. Example:

            orchestrator -c start-subordinate -i subordinate.to.be.started.com

        restart-subordinate
            Issues STOP SLAVE + START SLAVE; Example:

            orchestrator -c restart-subordinate -i subordinate.to.be.started.com

        skip-query
            On a failed replicating subordinate, skips a single query and attempts to resume replication.
            Only applies when the replication seems to be broken on SQL thread (e.g. on duplicate
            key error). Also works in GTID mode. Example:

            orchestrator -c skip-query -i subordinate.with.broken.sql.thread.com

        reset-subordinate
            Issues a RESET SLAVE command. Destructive to replication. Example:

            orchestrator -c reset-subordinate -i subordinate.to.reset.com

        detach-subordinate
            Stops replication and modifies binlog position into an impossible, yet reversible, value.
            This effectively means the replication becomes broken. See reattach-subordinate. Example:

            orchestrator -c detach-subordinate -i subordinate.whose.replication.will.break.com

            Issuing this on an already detached subordinate will do nothing.

        reattach-subordinate
            Undo a detach-subordinate operation. Reverses the binlog change into the original values, and
            resumes replication. Example:

            orchestrator -c reattach-subordinate -i detahced.subordinate.whose.replication.will.amend.com

            Issuing this on an attached (i.e. normal) subordinate will do nothing.

        detach-subordinate-main-host
            Stops replication and modifies Main_Host into an impossible, yet reversible, value.
            This effectively means the replication becomes broken. See reattach-subordinate-main-host. Example:

            orchestrator -c detach-subordinate-main-host -i subordinate.whose.replication.will.break.com

            Issuing this on an already detached subordinate will do nothing.

        reattach-subordinate-main-host
            Undo a detach-subordinate-main-host operation. Reverses the hostname change into the original value, and
            resumes replication. Example:

            orchestrator -c reattach-subordinate-main-host -i detahced.subordinate.whose.replication.will.amend.com

            Issuing this on an attached (i.e. normal) subordinate will do nothing.

		restart-subordinate-statements
			Prints a list of statements to execute to stop then restore subordinate to same execution state.
			Provide --statement for injected statement.
			This is useful for issuing a command that can only be executed whiel subordinate is stopped. Such
			commands are any of CHANGE MASTER TO.
			Orchestrator will not execute given commands, only print them as courtesy. It may not have
			the privileges to execute them in the first place. Example:

			orchestrator -c restart-subordinate-statements -i some.subordinate.com -statement="change main to main_heartbeat_period=5"

    General instance commands
        Applying general instance configuration and state

        set-read-only
            Turn an instance read-only, via SET GLOBAL read_only := 1. Examples:

            orchestrator -c set-read-only -i instance.to.turn.read.only.com

            orchestrator -c set-read-only
                -i not given, implicitly assumed local hostname

        set-writeable
            Turn an instance writeable, via SET GLOBAL read_only := 0. Example:

            orchestrator -c set-writeable -i instance.to.turn.writeable.com

            orchestrator -c set-writeable
                -i not given, implicitly assumed local hostname

    Binlog commands
        Commands that investigate/work on binary logs

        flush-binary-logs
            Flush binary logs on an instance. Examples:

            orchestrator -c flush-binary-logs -i instance.with.binary.logs.com

            orchestrator -c flush-binary-logs -i instance.with.binary.logs.com --binlog=mysql-bin.002048
                Flushes binary logs until reaching given number. Fails when current number is larger than input

        purge-binary-logs
            Purge binary logs on an instance. Examples:

            orchestrator -c purge-binary-logs -i instance.with.binary.logs.com --binlog mysql-bin.002048

                Purges binary logs until given log

        last-pseudo-gtid
            Information command; an authoritative way of detecting whether a Pseudo-GTID event exist for an instance,
            and if so, output the last Pseudo-GTID entry and its location. Example:

            orchestrator -c last-pseudo-gtid -i instance.with.possible.pseudo-gtid.injection

        find-binlog-entry
            Get binlog file:pos of entry given by --pattern (exact full match, not a regular expression) in a given instance.
            This will search the instance's binary logs starting with most recent, and terminate as soon as an exact match is found.
            The given input is not a regular expression. It must fully match the entry (not a substring).
            This is most useful when looking for uniquely identifyable values, such as Pseudo-GTID. Example:

            orchestrator -c find-binlog-entry -i instance.to.search.on.com --pattern "insert into my_data (my_column) values ('distinct_value_01234_56789')"

                Prints out the binlog file:pos where the entry is found, or errors if unfound.

        correlate-binlog-pos
            Given an instance (-i) and binlog coordinates (--binlog=file:pos), find the correlated coordinates in another instance (-d).
            "Correlated coordinates" are those that present the same point-in-time of sequence of binary log events, untangling
            the mess of different binlog file:pos coordinates on different servers.
            This operation relies on Pseudo-GTID: your servers must have been pre-injected with PSeudo-GTID entries as these are
            being used as binlog markers in the correlation process.
            You must provide a valid file:pos in the binlogs of the source instance (-i), and in response get the correlated
            coordinates in the binlogs of the destination instance (-d). This operation does not work on relay logs.
            Example:

            orchestrator -c correlate-binlog-pos  -i instance.with.binary.log.com --binlog=mysql-bin.002366:14127 -d other.instance.with.binary.logs.com

                Prints out correlated coordinates, e.g.: "mysql-bin.002302:14220", or errors out.

    Pool commands
        Orchestrator provides with getter/setter commands for handling pools. It does not on its own investigate pools,
        but merely accepts and provides association of an instance (host:port) and a pool (any_name).

        submit-pool-instances
            Submit a pool name with a list of instances in that pool. This removes any previous instances associated with
            that pool. Expecting comma delimited list of instances

            orchestrator -c submit-pool-instances --pool name_of_pool -i pooled.instance1.com,pooled.instance2.com:3306,pooled.instance3.com

        cluster-pool-instances
            List all pools and their associated instances. Output is in tab delimited format, and lists:
            cluster_name, cluster_alias, pool_name, pooled instance
            Example:

            orchestrator -c cluster-pool-instances

				which-heuristic-cluster-pool-instances
						List instances belonging to a cluster, which are also in some pool or in a specific given pool.
						Not all instances are listed: unreachable, downtimed instances ar left out. Only those that should be
						responsive and healthy are listed. This serves applications in getting information about instances
						that could be queried (this complements a proxy behavior in providing the *list* of instances).
						Examples:

						orchestrator -c which-heuristic-cluster-pool-instances --alias mycluster
								Get the instances of a specific cluster, no specific pool

						orchestrator -c which-heuristic-cluster-pool-instances --alias mycluster --pool some_pool
								Get the instances of a specific cluster and which belong to a given pool

						orchestrator -c which-heuristic-cluster-pool-instances -i instance.belonging.to.a.cluster
								Cluster inferred by given instance

						orchestrator -c which-heuristic-cluster-pool-instances
								Cluster inferred by local hostname

    Information commands
        These commands provide/store information about topologies, replication connections, or otherwise orchstrator's
        "inventory".

        find
            Find instances whose hostname matches given regex pattern. Example:

            orchestrator -c find -pattern "backup.*us-east"

        clusters
            List all clusters known to orchestrator. A cluster (aka topology, aka chain) is identified by its
            main (or one of its main if more than one exists). Example:

            orchesrtator -c clusters
                -i not given, implicitly assumed local hostname

        all-clusters-mains
            List of writeable mains, one per cluster.
			For most single-main topologies, this is trivially the main.
			For active-active main-main topologies, this ensures only one of
			the mains is returned.

			Example:

            orchestrator -c all-clusters-mains

        topology
            Show an ascii-graph of a replication topology, given a member of that topology. Example:

            orchestrator -c topology -i instance.belonging.to.a.topology.com

            orchestrator -c topology
                -i not given, implicitly assumed local hostname

            Instance must be already known to orchestrator. Topology is generated by orchestrator's mapping
            and not from synchronuous investigation of the instances. The generated topology may include
            instances that are dead, or whose replication is broken.

        all-instances
            List the complete known set of instances. Similar to '-c find -pattern "."'
			Example:

            orchestrator -c all-instances

        which-instance
            Output the fully-qualified hostname:port representation of the given instance, or error if unknown
            to orchestrator. Examples:

            orchestrator -c which-instance -i instance.to.check.com

            orchestrator -c which-instance
                -i not given, implicitly assumed local hostname

        which-cluster
            Output the name of the cluster an instance belongs to, or error if unknown to orchestrator. Examples:

            orchestrator -c which-cluster -i instance.to.check.com

            orchestrator -c which-cluster
                -i not given, implicitly assumed local hostname

        which-cluster-instances
            Output the list of instances participating in same cluster as given instance; output is one line
            per instance, in hostname:port format. Examples:

            orchestrator -c which-cluster-instances -i instance.to.check.com

            orchestrator -c which-cluster-instances
                -i not given, implicitly assumed local hostname

            orchestrator -c which-cluster-instances -alias some_alias
                assuming some_alias is a known cluster alias (see ClusterNameToAlias or DetectClusterAliasQuery configuration)

        which-cluster-domain
            Output the domain name of given cluster, indicated by instance or alias. This depends on
						the DetectClusterDomainQuery configuration. Example:

            orchestrator -c which-cluster-domain -i instance.to.check.com

            orchestrator -c which-cluster-domain
                -i not given, implicitly assumed local hostname

            orchestrator -c which-cluster-domain -alias some_alias
                assuming some_alias is a known cluster alias (see ClusterNameToAlias or DetectClusterAliasQuery configuration)

				which-heuristic-domain-instance
						Returns the instance associated as the cluster's writer with a cluster's domain name.
						Given a cluster, orchestrator looks for the domain name indicated by this cluster, and proceeds to search for
						a stord key-value attribute for that domain name. This would be the writer host for the given domain.
						See also set-heuristic-domain-instance, this is meant to be a temporary service mimicking in micro-scale a
						service discovery functionality.
						Example:

						orchestrator -c which-heuristic-domain-instance -alias some_alias
							Detects the domain name for given cluster, reads from key-value store the writer host associated with the domain name.

						orchestrator -c which-heuristic-domain-instance -i instance.of.some.cluster
							Cluster is inferred by a member instance (the instance is not necessarily the main)

				which-cluster-main
						Output the name of the active main in a given cluster, indicated by instance or alias.
						An "active" main is one that is writable and is not marked as downtimed due to a topology recovery.
						Examples:

            orchestrator -c which-cluster-main -i instance.to.check.com

            orchestrator -c which-cluster-main
                -i not given, implicitly assumed local hostname

            orchestrator -c which-cluster-main -alias some_alias
                assuming some_alias is a known cluster alias (see ClusterNameToAlias or DetectClusterAliasQuery configuration)

        which-cluster-osc-subordinates
            Output a list of subordinates in same cluster as given instance, that would server as good candidates as control subordinates
            for a pt-online-schema-change operation.
            Those subordinates would be used for replication delay so as to throtthe osc operation. Selected subordinates will include,
            where possible: intermediate mains, their subordinates, 3rd level subordinates, direct non-intermediate-main subordinates.

            orchestrator -c which-cluster-osc-subordinates -i instance.to.check.com

            orchestrator -c which-cluster-osc-subordinates
                -i not given, implicitly assumed local hostname

            orchestrator -c which-cluster-osc-subordinates -alias some_alias
                assuming some_alias is a known cluster alias (see ClusterNameToAlias or DetectClusterAliasQuery configuration)

				which-lost-in-recovery
						List instances marked as downtimed for being lost in a recovery process. This depends on the configuration
						of MainFailoverLostInstancesDowntimeMinutes. The output of this command lists heuristically recent
						"lost" instances that probabaly should be recycled. Note that when the 'downtime' flag expires (or
						is reset by '-c end-downtime') an instance no longer appears on this list.
						The topology recovery process injects a magic hint when downtiming lost instances, that is picked up
						by this command. Examples:

						orchestrator -c which-lost-in-recovery
								Lists all heuristically-recent known lost instances

        which-main
            Output the fully-qualified hostname:port representation of a given instance's main. Examples:

            orchestrator -c which-main -i a.known.subordinate.com

            orchestrator -c which-main
                -i not given, implicitly assumed local hostname

        which-subordinates
            Output the fully-qualified hostname:port list of subordinates (one per line) of a given instance (or empty
            list if instance is not a main to anyone). Examples:

            orchestrator -c which-subordinates -i a.known.instance.com

            orchestrator -c which-subordinates
                -i not given, implicitly assumed local hostname

        get-cluster-heuristic-lag
            For a given cluster (indicated by an instance or alias), output a heuristic "representative" lag of that cluster.
            The output is obtained by examining the subordinates that are member of "which-cluster-osc-subordinates"-command, and
            getting the maximum subordinate lag of those subordinates. Recall that those subordinates are a subset of the entire cluster,
            and that they are ebing polled periodically. Hence the output of this command is not necessarily up-to-date
            and does not represent all subordinates in cluster. Examples:

            orchestrator -c get-cluster-heuristic-lag -i instance.that.is.part.of.cluster.com

            orchestrator -c get-cluster-heuristic-lag
                -i not given, implicitly assumed local host, cluster implied

            orchestrator -c get-cluster-heuristic-lag -alias some_alias
                assuming some_alias is a known cluster alias (see ClusterNameToAlias or DetectClusterAliasQuery configuration)

        instance-status
            Output short status on a given instance (name, replication status, noteable configuration). Example2:

            orchestrator -c instance-status -i instance.to.investigate.com

            orchestrator -c instance-status
                -i not given, implicitly assumed local hostname

        snapshot-topologies
            Take a snapshot of existing topologies. This will record minimal replication topology data: the identity
            of an instance, its main and its cluster.
            Taking a snapshot later allows for reviewing changes in topologies. One might wish to invoke this command
            on a daily basis, and later be able to solve questions like 'where was this instacne replicating from before
            we moved it?', 'which instances were replication from this instance a week ago?' etc. Example:

            orchestrator -c snapshot-topologies

    Orchestrator instance management
        These command dig into the way orchestrator manages instances and operations on instances

        discover
            Request that orchestrator cotacts given instance, reads its status, and upsert it into
            orchestrator's respository. Examples:

            orchestrator -c discover -i instance.to.discover.com:3306

            orchestrator -c discover -i cname.of.instance

            orchestrator -c discover
                -i not given, implicitly assumed local hostname

            Orchestrator will resolve CNAMEs and VIPs.

        forget
            Request that orchestrator removed given instance from its repository. If the instance is alive
            and connected through replication to otherwise known and live instances, orchestrator will
            re-discover it by nature of its discovery process. Instances are auto-removed via config's
            UnseenAgentForgetHours. If you happen to know a machine is decommisioned, for example, it
            can be nice to remove it from the repository before it auto-expires. Example:

            orchestrator -c forget -i instance.to.forget.com

            Orchestrator will *not* resolve CNAMEs and VIPs for given instance.

        begin-maintenance
            Request a maintenance lock on an instance. Topology changes require placing locks on the minimal set of
            affected instances, so as to avoid an incident of two uncoordinated operations on a smae instance (leading
            to possible chaos). Locks are placed in the backend database, and so multiple orchestrator instances are safe.
            Operations automatically acquire locks and release them. This command manually acquires a lock, and will
            block other operations on the instance until lock is released.
            Note that orchestrator automatically assumes locks to be expired after MaintenanceExpireMinutes (in config).
            Examples:

            orchestrator -c begin-maintenance -i instance.to.lock.com --duration=3h --reason="load testing; do not disturb"
                accepted duration format: 10s, 30m, 24h, 3d, 4w

            orchestrator -c begin-maintenance -i instance.to.lock.com --reason="load testing; do not disturb"
                --duration not given; default to config's MaintenanceExpireMinutes

        end-maintenance
            Remove maintenance lock; such lock may have been gained by an explicit begin-maintenance command implicitly
            by a topology change. You should generally only remove locks you have placed manually; orchestrator will
            automatically expire locks after MaintenanceExpireMinutes (in config).
            Example:

            orchestrator -c end-maintenance -i locked.instance.com

        begin-downtime
            Mark an instance as downtimed. A downtimed instance is assumed to be taken care of, and recovery-analysis does
            not apply for such an instance. As result, no recommendation for recovery, and no automated-recovery are issued
            on a downtimed instance.
            Downtime is different than maintanence in that it places no lock (mainenance uses an exclusive lock on the instance).
            It is OK to downtime an instance that is already downtimed -- the new begin-downtime command will override whatever
            previous downtime attributes there were on downtimes instance.
            Note that orchestrator automatically assumes downtime to be expired after MaintenanceExpireMinutes (in config).
            Examples:

            orchestrator -c begin-downtime -i instance.to.downtime.com --duration=3h --reason="dba handling; do not do recovery"
                accepted duration format: 10s, 30m, 24h, 3d, 4w

            orchestrator -c begin-downtime -i instance.to.lock.com --reason="dba handling; do not do recovery"
                --duration not given; default to config's MaintenanceExpireMinutes

        end-downtime
            Indicate an instance is no longer downtimed. Typically you should not need to use this since
            a downtime is always bounded by a duration and auto-expires. But you may use this to forcibly
            indicate the active downtime should be expired now.
            Example:

            orchestrator -c end-downtime -i downtimed.instance.com

    Crash recovery commands

        recover
            Do auto-recovery given a dead instance. Orchestrator chooses the best course of action.
            The given instance must be acknowledged as dead and have subordinates, or else there's nothing to do.
            See "replication-analysis" command.
            Orchestrator executes external processes as configured by *Processes variables.
            --debug is your friend. Example:

            orchestrator -c recover -i dead.instance.com --debug

        recover-lite
            Do auto-recovery given a dead instance. Orchestrator chooses the best course of action, exactly
            as in "-c recover". Orchestratir will *not* execute external processes.

            orchestrator -c recover-lite -i dead.instance.com --debug

				force-main-takeover
						Forcibly discard main and promote another (direct child) instance instead, even if everything is running well.
						This allows for planned switchover.
						NOTE:
						- You must specify the instance to promote via "-d"
						- Promoted instance must be a direct child of the existing main
						- This will not work in a main-main configuration
						- Orchestrator just treats this command as a DeadMain failover scenario
						- It is STRONGLY suggested that you first relocate everything below your chosen instance-to-promote.
						  It *is* a planned failover thing.
						- Otherwise orchestrator will do its thing in moving instances around, hopefully promoting your requested
						  server on top.
						- Orchestrator will issue all relevant pre-failover and post-failover external processes.
						- In this command orchestrator will not issue 'SET GLOBAL read_only=1' on the existing main, nor will
						  it issue a 'FLUSH TABLES WITH READ LOCK'. Please see the 'graceful-main-takeover' command.
						Examples:

						orchestrator -c force-main-takeover -alias mycluster -d immediate.child.of.main.com
								Indicate cluster by alias. Orchestrator automatically figures out the main

						orchestrator -c force-main-takeover -i instance.in.relevant.cluster.com -d immediate.child.of.main.com
								Indicate cluster by an instance. You don't structly need to specify the main, orchestrator
								will infer the main's identify.

				graceful-main-takeover
						Gracefully discard main and promote another (direct child) instance instead, even if everything is running well.
						This allows for planned switchover.
						NOTE:
						- Promoted instance must be a direct child of the existing main
						- Promoted instance must be the *only* direct child of the existing main. It *is* a planned failover thing.
						- Orchestrator will first issue a "set global read_only=1" on existing main
						- It will promote candidate main to the binlog positions of the existing main after issuing the above
						- There _could_ still be statements issued and executed on the existing main by SUPER users, but those are ignored.
						- Orchestrator then proceeds to handle a DeadMain failover scenario
						- Orchestrator will issue all relevant pre-failover and post-failover external processes.
						Examples:

						orchestrator -c graceful-main-takeover -alias mycluster
								Indicate cluster by alias. Orchestrator automatically figures out the main and verifies it has a single direct replica

						orchestrator -c force-main-takeover -i instance.in.relevant.cluster.com
								Indicate cluster by an instance. You don't structly need to specify the main, orchestrator
								will infer the main's identify.

        replication-analysis
            Request an analysis of potential crash incidents in all known topologies.
            Output format is not yet stabilized and may change in the future. Do not trust the output
            for automated parsing. Use web API instead, at this time. Example:

            orchestrator -c replication-analysis

        ack-cluster-recoveries
            Acknowledge recoveries for a given cluster; this unblocks pending future recoveries.
            Acknowledging a recovery requires a comment (supply via --reason). Acknowledgement clears the in-active-period
            flag for affected recoveries, which in turn affects any blocking recoveries.
            Multiple recoveries may be affected. Only unacknowledged recoveries will be affected.
            Examples:

            orchestrator -c ack-cluster-recoveries -i instance.in.a.cluster.com --reason="dba has taken taken necessary steps"
                 Cluster is indicated by any of its members. The recovery need not necessarily be on/to given instance.

            orchestrator -c ack-cluster-recoveries -alias some_alias --reason="dba has taken taken necessary steps"
                 Cluster indicated by alias

        ack-instance-recoveries
            Acknowledge recoveries for a given instance; this unblocks pending future recoveries.
            Acknowledging a recovery requires a comment (supply via --reason). Acknowledgement clears the in-active-period
            flag for affected recoveries, which in turn affects any blocking recoveries.
            Multiple recoveries may be affected. Only unacknowledged recoveries will be affected.
            Example:

            orchestrator -c ack-cluster-recoveries -i instance.that.failed.com --reason="dba has taken taken necessary steps"

    Instance meta commands

        register-candidate
            Indicate that a specific instance is a preferred candidate for main promotion. Upon a dead main
            recovery, orchestrator will do its best to promote instances that are marked as candidates. However
            orchestrator cannot guarantee this will always work. Issues like version compatabilities, binlog format
            etc. are limiting factors.
            You will want to mark an instance as a candidate when: it is replicating directly from the main, has
            binary logs and log_subordinate_updates is enabled, uses same binlog_format as its siblings, compatible version
            as its siblings. If you're using DataCenterPattern & PhysicalEnvironmentPattern (see configuration),
            you would further wish to make sure you have a candidate in each data center.
            Orchestrator first promotes the best-possible subordinate, and only then replaces it with your candidate,
            and only if both in same datcenter and physical enviroment.
            An instance needs to continuously be marked as candidate, so as to make sure orchestrator is not wasting
            time with stale instances. Orchestrator periodically clears candidate-registration for instances that have
            not been registeres for over CandidateInstanceExpireMinutes (see config).
            Example:

            orchestrator -c register-candidate -i candidate.instance.com

            orchestrator -c register-candidate
                -i not given, implicitly assumed local hostname

        register-hostname-unresolve
            Assigns the given instance a virtual (aka "unresolved") name. When moving subordinates under an instance with assigned
            "unresolve" name, orchestrator issues a CHANGE MASTER TO MASTER_HOST='<the unresovled name instead of the fqdn>' ...
            This is useful in cases where your main is behind virtual IP (e.g. active/passive mains with shared storage or DRBD,
            e.g. binlog servers sharing common VIP).
            A "repoint" command is useful after "register-hostname-unresolve": you can repoint subordinates of the instance to their exact
            same location, and orchestrator will swap the fqdn of their main with the unresolved name.
            Such registration must be periodic. Orchestrator automatically expires such registration after ExpiryHostnameResolvesMinutes.
            Example:

            orchestrator -c register-hostname-unresolve -i instance.fqdn.com --hostname=virtual.name.com

        deregister-hostname-unresolve
            Explicitly deregister/dosassociate a hostname with an "unresolved" name. Orchestrator merely remvoes the association, but does
            not touch any subordinate at this point. A "repoint" command can be useful right after calling this command to change subordinate's main host
            name (assumed to be an "unresolved" name, such as a VIP) with the real fqdn of the main host.
            Example:

            orchestrator -c deregister-hostname-unresolve -i instance.fqdn.com

				set-heuristic-domain-instance
						This is a temporary (sync your watches, watch for next ice age) command which registers the cluster domain name of a given cluster
						with the main/writer host for that cluster. It is a one-time-main-discovery operation.
						At this time orchestrator may also act as a small & simple key-value store (recall the "temporary" indication).
						Main failover operations will overwrite the domain instance identity. Orchestrator so turns into a mini main-discovery
						service (I said "TEMPORARY"). Really there are other tools for the job. See also: which-heuristic-domain-instance
						Example:

						orchestrator -c set-heuristic-domain-instance --alias some_alias
								Detects the domain name for given cluster, identifies the writer main of the cluster, associates the two in key-value store

						orchestrator -c set-heuristic-domain-instance -i instance.of.some.cluster
								Cluster is inferred by a member instance (the instance is not necessarily the main)

    Misc commands

        continuous
            Enter continuous mode, and actively poll for instances, diagnose problems, do maintenance etc.
            This type of work is typically done in HTTP mode. However nothing prevents orchestrator from
            doing it in command line. Invoking with "continuous" will run indefinitely. Example:

            orchestrator -c continuous

				active-nodes
						List orchestrator nodes or processes that are actively running or have most recently
						executed. Output is in hostname:token format, where "token" is an internal unique identifier
						of an orchestrator process. Example:

						orchestrator -c active-nodes

				access-token
						When running HTTP with "AuthenticationMethod" : "token", receive a new access token.
						This token must be utilized within "AccessTokenUseExpirySeconds" and can then be used
						until "AccessTokenExpiryMinutes" have passed.
						In "token" authentication method a user is read-only unless able to provide with a fresh token.
						A token may only be used once (two users must get two distinct tokens).
						Submitting a token is done via "/web/access-token?publicToken=<received_token>". The token is then stored
						in HTTP cookie.

						orchestrator -c access-token

        reset-hostname-resolve-cache
            Clear the hostname resolve cache; it will be refilled by following host discoveries

            orchestrator -c reset-hostname-resolve-cache

        resolve
            Utility command to resolve a CNAME and return resolved hostname name. Example:

            orchestrator -c resolve -i cname.to.resolve

        redeploy-internal-db
						Force internal schema migration to current backend structure. Orchestrator keeps track of the deployed
						versions and will not reissue a migration for a version already deployed. Normally you should not use
						this command, and it is provided mostly for building and testing purposes. Nonetheless it is safe to
						use and at most it wastes some cycles.
    `

// main is the application's entry point. It will either spawn a CLI or HTTP itnerfaces.
func main() {
	configFile := flag.String("config", "", "config file name")
	command := flag.String("c", "", "command, required. See full list of commands via 'orchestrator -c help'")
	strict := flag.Bool("strict", false, "strict mode (more checks, slower)")
	instance := flag.String("i", "", "instance, host_fqdn[:port] (e.g. db.company.com:3306, db.company.com)")
	sibling := flag.String("s", "", "sibling instance, host_fqdn[:port]")
	destination := flag.String("d", "", "destination instance, host_fqdn[:port] (synonym to -s)")
	owner := flag.String("owner", "", "operation owner")
	reason := flag.String("reason", "", "operation reason")
	duration := flag.String("duration", "", "maintenance duration (format: 59s, 59m, 23h, 6d, 4w)")
	pattern := flag.String("pattern", "", "regular expression pattern")
	clusterAlias := flag.String("alias", "", "cluster alias")
	pool := flag.String("pool", "", "Pool logical name (applies for pool-related commands)")
	hostnameFlag := flag.String("hostname", "", "Hostname/fqdn/CNAME/VIP (applies for hostname/resolve related commands)")
	discovery := flag.Bool("discovery", true, "auto discovery mode")
	quiet := flag.Bool("quiet", false, "quiet")
	verbose := flag.Bool("verbose", false, "verbose")
	debug := flag.Bool("debug", false, "debug mode (very verbose)")
	stack := flag.Bool("stack", false, "add stack trace upon error")
	config.RuntimeCLIFlags.Databaseless = flag.Bool("databaseless", false, "EXPERIMENTAL! Work without backend database")
	config.RuntimeCLIFlags.SkipUnresolve = flag.Bool("skip-unresolve", false, "Do not unresolve a host name")
	config.RuntimeCLIFlags.SkipUnresolveCheck = flag.Bool("skip-unresolve-check", false, "Skip/ignore checking an unresolve mapping (via hostname_unresolve table) resolves back to same hostname")
	config.RuntimeCLIFlags.Noop = flag.Bool("noop", false, "Dry run; do not perform destructing operations")
	config.RuntimeCLIFlags.BinlogFile = flag.String("binlog", "", "Binary log file name")
	config.RuntimeCLIFlags.Statement = flag.String("statement", "", "Statement/hint")
	config.RuntimeCLIFlags.GrabElection = flag.Bool("grab-election", false, "Grab leadership (only applies to continuous mode)")
	config.RuntimeCLIFlags.PromotionRule = flag.String("promotion-rule", "prefer", "Promotion rule for register-andidate (prefer|neutral|must_not)")
	config.RuntimeCLIFlags.Version = flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *destination != "" && *sibling != "" {
		log.Fatalf("-s and -d are synonyms, yet both were specified. You're probably doing the wrong thing.")
	}
	switch *config.RuntimeCLIFlags.PromotionRule {
	case "prefer", "neutral", "must_not":
		{
			// OK
		}
	default:
		{
			log.Fatalf("-promotion-rule only supports prefer|neutral|must_not")
		}
	}
	if *destination == "" {
		*destination = *sibling
	}

	log.SetLevel(log.ERROR)
	if *verbose {
		log.SetLevel(log.INFO)
	}
	if *debug {
		log.SetLevel(log.DEBUG)
	}
	if *stack {
		log.SetPrintStackTrace(*stack)
	}
	log.Info("starting orchestrator") // FIXME and add the version which is currently in build.sh

	if *config.RuntimeCLIFlags.Version {
		fmt.Println(AppVersion)
		return
	}

	runtime.GOMAXPROCS(math.MinInt(4, runtime.NumCPU()))

	if len(*configFile) > 0 {
		config.ForceRead(*configFile)
	} else {
		config.Read("/etc/orchestrator.conf.json", "conf/orchestrator.conf.json", "orchestrator.conf.json")
	}
	if *config.RuntimeCLIFlags.Databaseless {
		config.Config.DatabaselessMode__experimental = true
	}
	if config.Config.Debug {
		log.SetLevel(log.DEBUG)
	}
	if *quiet {
		// Override!!
		log.SetLevel(log.ERROR)
	}
	if config.Config.EnableSyslog {
		log.EnableSyslogWriter("orchestrator")
		log.SetSyslogLevel(log.INFO)
	}
	if config.Config.AuditToSyslog {
		inst.EnableAuditSyslog()
	}
	config.RuntimeCLIFlags.ConfiguredVersion = AppVersion

	inst.InitializeInstanceDao()

	if len(flag.Args()) == 0 && *command == "" {
		// No command, no argument: just prompt
		fmt.Println(prompt)
		return
	}
	switch {
	case len(flag.Args()) == 0 || flag.Arg(0) == "cli":
		app.CliWrapper(*command, *strict, *instance, *destination, *owner, *reason, *duration, *pattern, *clusterAlias, *pool, *hostnameFlag)
	case flag.Arg(0) == "http":
		app.Http(*discovery)
	default:
		fmt.Fprintln(os.Stderr, `Usage:
  orchestrator --options... [cli|http]
See complete list of commands:
  orchestrator -c help
Full blown documentation:
  orchestrator
`)
		os.Exit(1)
	}
}
