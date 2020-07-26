/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package inst

import (
	"fmt"
	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
	"github.com/outbrain/orchestrator/go/config"
	"github.com/outbrain/orchestrator/go/db"
	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
	"regexp"
	"time"
)

var analysisChangeWriteAttemptCounter = metrics.NewCounter()
var analysisChangeWriteCounter = metrics.NewCounter()

func init() {
	metrics.Register("analysis.change.write.attempt", analysisChangeWriteAttemptCounter)
	metrics.Register("analysis.change.write", analysisChangeWriteCounter)
}

var recentInstantAnalysis = cache.New(time.Duration(config.Config.RecoveryPollSeconds*2)*time.Second, time.Second)

// GetReplicationAnalysis will check for replication problems (dead main; unreachable main; etc)
func GetReplicationAnalysis(clusterName string, includeDowntimed bool, auditAnalysis bool) ([]ReplicationAnalysis, error) {
	result := []ReplicationAnalysis{}

	args := sqlutils.Args(config.Config.InstancePollSeconds, clusterName)
	analysisQueryReductionClause := ``
	if config.Config.ReduceReplicationAnalysisCount {
		analysisQueryReductionClause = `
			HAVING
				(MIN(
		        		main_instance.last_checked <= main_instance.last_seen
		        		AND main_instance.last_attempted_check <= main_instance.last_seen + INTERVAL (2 * ?) SECOND
		        	) IS TRUE /* AS is_last_check_valid */) = 0
				OR (IFNULL(SUM(subordinate_instance.last_checked <= subordinate_instance.last_seen
		                    AND subordinate_instance.subordinate_io_running = 0
		                    AND subordinate_instance.last_io_error RLIKE 'error (connecting|reconnecting) to main'
		                    AND subordinate_instance.subordinate_sql_running = 1),
		                0) /* AS count_subordinates_failing_to_connect_to_main */ > 0)
				OR (IFNULL(SUM(subordinate_instance.last_checked <= subordinate_instance.last_seen),
		                0) /* AS count_valid_subordinates */ < COUNT(subordinate_instance.server_id) /* AS count_subordinates */)
				OR (IFNULL(SUM(subordinate_instance.last_checked <= subordinate_instance.last_seen
		                    AND subordinate_instance.subordinate_io_running != 0
		                    AND subordinate_instance.subordinate_sql_running != 0),
		                0) /* AS count_valid_replicating_subordinates */ < COUNT(subordinate_instance.server_id) /* AS count_subordinates */)
				OR (MIN(
		            main_instance.subordinate_sql_running = 1
		            AND main_instance.subordinate_io_running = 0
		            AND main_instance.last_io_error RLIKE 'error (connecting|reconnecting) to main'
		          ) /* AS is_failing_to_connect_to_main */)
				OR (COUNT(subordinate_instance.server_id) /* AS count_subordinates */ > 0)
			`
		args = append(args, config.Config.InstancePollSeconds)
	}
	// "OR count_subordinates > 0" above is a recent addition, which, granted, makes some previous conditions redundant.
	// It gives more output, and more "NoProblem" messages that I am now interested in for purpose of auditing in database_instance_analysis_changelog
	query := fmt.Sprintf(`
		    SELECT
		        main_instance.hostname,
		        main_instance.port,
		        MIN(main_instance.main_host) AS main_host,
		        MIN(main_instance.main_port) AS main_port,
		        MIN(main_instance.cluster_name) AS cluster_name,
		        MIN(IFNULL(cluster_alias.alias, main_instance.cluster_name)) AS cluster_alias,
		        MIN(
		        		main_instance.last_checked <= main_instance.last_seen
		        		AND main_instance.last_attempted_check <= main_instance.last_seen + INTERVAL (2 * ?) SECOND
		        	) IS TRUE AS is_last_check_valid,
		        MIN(main_instance.main_host IN ('' , '_')
		            OR main_instance.main_port = 0
								OR left(main_instance.main_host, 2) = '//') AS is_main,
		        MIN(main_instance.is_co_main) AS is_co_main,
		        MIN(CONCAT(main_instance.hostname,
		                ':',
		                main_instance.port) = main_instance.cluster_name) AS is_cluster_main,
		        COUNT(subordinate_instance.server_id) AS count_subordinates,
		        IFNULL(SUM(subordinate_instance.last_checked <= subordinate_instance.last_seen),
		                0) AS count_valid_subordinates,
		        IFNULL(SUM(subordinate_instance.last_checked <= subordinate_instance.last_seen
		                    AND subordinate_instance.subordinate_io_running != 0
		                    AND subordinate_instance.subordinate_sql_running != 0),
		                0) AS count_valid_replicating_subordinates,
		        IFNULL(SUM(subordinate_instance.last_checked <= subordinate_instance.last_seen
		                    AND subordinate_instance.subordinate_io_running = 0
		                    AND subordinate_instance.last_io_error RLIKE 'error (connecting|reconnecting) to main'
		                    AND subordinate_instance.subordinate_sql_running = 1),
		                0) AS count_subordinates_failing_to_connect_to_main,
						IFNULL(SUM(
									current_relay_log_file=prev_relay_log_file
									and current_relay_log_pos=prev_relay_log_pos
									and current_seen != prev_seen),
								0) AS count_stale_subordinates,
		        MIN(main_instance.replication_depth) AS replication_depth,
		        GROUP_CONCAT(subordinate_instance.Hostname, ':', subordinate_instance.Port) as subordinate_hosts,
		        MIN(
		            main_instance.subordinate_sql_running = 1
		            AND main_instance.subordinate_io_running = 0
		            AND main_instance.last_io_error RLIKE 'error (connecting|reconnecting) to main'
		          ) AS is_failing_to_connect_to_main,
		        MIN(
				    		database_instance_downtime.downtime_active IS NULL
				    		OR database_instance_downtime.end_timestamp < NOW()
			    		) IS FALSE AS is_downtimed,
			    	MIN(
				    		IFNULL(database_instance_downtime.end_timestamp, '')
				    	) AS downtime_end_timestamp,
			    	MIN(
				    		IFNULL(TIMESTAMPDIFF(SECOND, NOW(), database_instance_downtime.end_timestamp), 0)
				    	) AS downtime_remaining_seconds,
			    	MIN(
				    		main_instance.binlog_server
				    	) AS is_binlog_server,
			    	MIN(
				    		main_instance.pseudo_gtid
				    	) AS is_pseudo_gtid,
			    	MIN(
				    		main_instance.supports_oracle_gtid
				    	) AS supports_oracle_gtid,
			    	SUM(
				    		subordinate_instance.oracle_gtid
				    	) AS count_oracle_gtid_subordinates,
			      IFNULL(SUM(subordinate_instance.last_checked <= subordinate_instance.last_seen
	              AND subordinate_instance.oracle_gtid != 0),
              0) AS count_valid_oracle_gtid_subordinates,
			    	SUM(
				    		subordinate_instance.binlog_server
				    	) AS count_binlog_server_subordinates,
		        IFNULL(SUM(subordinate_instance.last_checked <= subordinate_instance.last_seen
                  AND subordinate_instance.binlog_server != 0),
              0) AS count_valid_binlog_server_subordinates,
			    	MIN(
				    		main_instance.mariadb_gtid
				    	) AS is_mariadb_gtid,
			    	SUM(
				    		subordinate_instance.mariadb_gtid
				    	) AS count_mariadb_gtid_subordinates,
		        IFNULL(SUM(subordinate_instance.last_checked <= subordinate_instance.last_seen
                  AND subordinate_instance.mariadb_gtid != 0),
              0) AS count_valid_mariadb_gtid_subordinates,
						IFNULL(SUM(subordinate_instance.log_bin
							  AND subordinate_instance.log_subordinate_updates
								AND subordinate_instance.binlog_format = 'STATEMENT'),
              0) AS count_statement_based_loggin_subordinates,
						IFNULL(SUM(subordinate_instance.log_bin
								AND subordinate_instance.log_subordinate_updates
								AND subordinate_instance.binlog_format = 'MIXED'),
              0) AS count_mixed_based_loggin_subordinates,
						IFNULL(SUM(subordinate_instance.log_bin
								AND subordinate_instance.log_subordinate_updates
								AND subordinate_instance.binlog_format = 'ROW'),
              0) AS count_row_based_loggin_subordinates,
						COUNT(DISTINCT IF(
							subordinate_instance.log_bin AND subordinate_instance.log_subordinate_updates,
								substring_index(subordinate_instance.version, '.', 2),
								NULL)
						) AS count_distinct_logging_major_versions
		    FROM
		        database_instance main_instance
		            LEFT JOIN
		        hostname_resolve ON (main_instance.hostname = hostname_resolve.hostname)
		            LEFT JOIN
		        database_instance subordinate_instance ON (COALESCE(hostname_resolve.resolved_hostname,
		                main_instance.hostname) = subordinate_instance.main_host
		            	AND main_instance.port = subordinate_instance.main_port)
		            LEFT JOIN
		        database_instance_maintenance ON (main_instance.hostname = database_instance_maintenance.hostname
		        		AND main_instance.port = database_instance_maintenance.port
		        		AND database_instance_maintenance.maintenance_active = 1)
		            LEFT JOIN
		        database_instance_downtime ON (main_instance.hostname = database_instance_downtime.hostname
		        		AND main_instance.port = database_instance_downtime.port
		        		AND database_instance_downtime.downtime_active = 1)
		        	LEFT JOIN
		        cluster_alias ON (cluster_alias.cluster_name = main_instance.cluster_name)
						  LEFT JOIN
						database_instance_recent_relaylog_history ON (
								subordinate_instance.hostname = database_instance_recent_relaylog_history.hostname
		        		AND subordinate_instance.port = database_instance_recent_relaylog_history.port)
		    WHERE
		    	database_instance_maintenance.database_instance_maintenance_id IS NULL
		    	AND ? IN ('', main_instance.cluster_name)
		    GROUP BY
			    main_instance.hostname,
			    main_instance.port
			%s
		    ORDER BY
			    is_main DESC ,
			    is_cluster_main DESC,
			    count_subordinates DESC
	`, analysisQueryReductionClause)
	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		a := ReplicationAnalysis{Analysis: NoProblem}

		a.IsMain = m.GetBool("is_main")
		a.IsCoMain = m.GetBool("is_co_main")
		a.AnalyzedInstanceKey = InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		a.AnalyzedInstanceMainKey = InstanceKey{Hostname: m.GetString("main_host"), Port: m.GetInt("main_port")}
		a.ClusterDetails.ClusterName = m.GetString("cluster_name")
		a.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		a.LastCheckValid = m.GetBool("is_last_check_valid")
		a.CountSubordinates = m.GetUint("count_subordinates")
		a.CountValidSubordinates = m.GetUint("count_valid_subordinates")
		a.CountValidReplicatingSubordinates = m.GetUint("count_valid_replicating_subordinates")
		a.CountSubordinatesFailingToConnectToMain = m.GetUint("count_subordinates_failing_to_connect_to_main")
		a.CountStaleSubordinates = m.GetUint("count_stale_subordinates")
		a.ReplicationDepth = m.GetUint("replication_depth")
		a.IsFailingToConnectToMain = m.GetBool("is_failing_to_connect_to_main")
		a.IsDowntimed = m.GetBool("is_downtimed")
		a.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
		a.DowntimeRemainingSeconds = m.GetInt("downtime_remaining_seconds")
		a.IsBinlogServer = m.GetBool("is_binlog_server")
		a.ClusterDetails.ReadRecoveryInfo()

		a.SubordinateHosts = *NewInstanceKeyMap()
		a.SubordinateHosts.ReadCommaDelimitedList(m.GetString("subordinate_hosts"))

		countValidOracleGTIDSubordinates := m.GetUint("count_valid_oracle_gtid_subordinates")
		a.OracleGTIDImmediateTopology = countValidOracleGTIDSubordinates == a.CountValidSubordinates && a.CountValidSubordinates > 0
		countValidMariaDBGTIDSubordinates := m.GetUint("count_valid_mariadb_gtid_subordinates")
		a.MariaDBGTIDImmediateTopology = countValidMariaDBGTIDSubordinates == a.CountValidSubordinates && a.CountValidSubordinates > 0
		countValidBinlogServerSubordinates := m.GetUint("count_valid_binlog_server_subordinates")
		a.BinlogServerImmediateTopology = countValidBinlogServerSubordinates == a.CountValidSubordinates && a.CountValidSubordinates > 0
		a.PseudoGTIDImmediateTopology = m.GetBool("is_pseudo_gtid")

		a.CountStatementBasedLoggingSubordinates = m.GetUint("count_statement_based_loggin_subordinates")
		a.CountMixedBasedLoggingSubordinates = m.GetUint("count_mixed_based_loggin_subordinates")
		a.CountRowBasedLoggingSubordinates = m.GetUint("count_row_based_loggin_subordinates")
		a.CountDistinctMajorVersionsLoggingSubordinates = m.GetUint("count_distinct_logging_major_versions")

		if a.IsMain && !a.LastCheckValid && a.CountSubordinates == 0 {
			a.Analysis = DeadMainWithoutSubordinates
			a.Description = "Main cannot be reached by orchestrator and has no subordinate"
			//
		} else if a.IsMain && !a.LastCheckValid && a.CountValidSubordinates == a.CountSubordinates && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = DeadMain
			a.Description = "Main cannot be reached by orchestrator and none of its subordinates is replicating"
			//
		} else if a.IsMain && !a.LastCheckValid && a.CountSubordinates > 0 && a.CountValidSubordinates == 0 && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = DeadMainAndSubordinates
			a.Description = "Main cannot be reached by orchestrator and none of its subordinates is replicating"
			//
		} else if a.IsMain && !a.LastCheckValid && a.CountValidSubordinates < a.CountSubordinates && a.CountValidSubordinates > 0 && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = DeadMainAndSomeSubordinates
			a.Description = "Main cannot be reached by orchestrator; some of its subordinates are unreachable and none of its reachable subordinates is replicating"
			//
		} else if a.IsMain && !a.LastCheckValid && a.CountStaleSubordinates == a.CountSubordinates && a.CountValidReplicatingSubordinates > 0 {
			a.Analysis = UnreachableMainWithStaleSubordinates
			a.Description = "Main cannot be reached by orchestrator and has running yet stale subordinates"
			//
		} else if a.IsMain && !a.LastCheckValid && a.CountValidSubordinates > 0 && a.CountValidReplicatingSubordinates > 0 {
			a.Analysis = UnreachableMain
			a.Description = "Main cannot be reached by orchestrator but it has replicating subordinates; possibly a network/host issue"
			//
		} else if a.IsMain && a.LastCheckValid && a.CountSubordinates == 1 && a.CountValidSubordinates == a.CountSubordinates && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = MainSingleSubordinateNotReplicating
			a.Description = "Main is reachable but its single subordinate is not replicating"
			//
		} else if a.IsMain && a.LastCheckValid && a.CountSubordinates == 1 && a.CountValidSubordinates == 0 {
			a.Analysis = MainSingleSubordinateDead
			a.Description = "Main is reachable but its single subordinate is dead"
			//
		} else if a.IsMain && a.LastCheckValid && a.CountSubordinates > 1 && a.CountValidSubordinates == a.CountSubordinates && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = AllMainSubordinatesNotReplicating
			a.Description = "Main is reachable but none of its subordinates is replicating"
			//
		} else if a.IsMain && a.LastCheckValid && a.CountSubordinates > 1 && a.CountValidSubordinates < a.CountSubordinates && a.CountValidSubordinates > 0 && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = AllMainSubordinatesNotReplicatingOrDead
			a.Description = "Main is reachable but none of its subordinates is replicating"
			//
		} else if a.IsMain && a.LastCheckValid && a.CountSubordinates > 1 && a.CountStaleSubordinates == a.CountSubordinates && a.CountValidSubordinates > 0 && a.CountValidReplicatingSubordinates > 0 {
			a.Analysis = AllMainSubordinatesStale
			a.Description = "Main is reachable but all of its subordinates are stale, although attempting to replicate"
			//
		} else /* co-main */ if a.IsCoMain && !a.LastCheckValid && a.CountSubordinates > 0 && a.CountValidSubordinates == a.CountSubordinates && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = DeadCoMain
			a.Description = "Co-main cannot be reached by orchestrator and none of its subordinates is replicating"
			//
		} else if a.IsCoMain && !a.LastCheckValid && a.CountSubordinates > 0 && a.CountValidSubordinates < a.CountSubordinates && a.CountValidSubordinates > 0 && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = DeadCoMainAndSomeSubordinates
			a.Description = "Co-main cannot be reached by orchestrator; some of its subordinates are unreachable and none of its reachable subordinates is replicating"
			//
		} else if a.IsCoMain && !a.LastCheckValid && a.CountValidSubordinates > 0 && a.CountValidReplicatingSubordinates > 0 {
			a.Analysis = UnreachableCoMain
			a.Description = "Co-main cannot be reached by orchestrator but it has replicating subordinates; possibly a network/host issue"
			//
		} else if a.IsCoMain && a.LastCheckValid && a.CountSubordinates > 0 && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = AllCoMainSubordinatesNotReplicating
			a.Description = "Co-main is reachable but none of its subordinates is replicating"
			//
		} else /* intermediate-main */ if !a.IsMain && !a.LastCheckValid && a.CountSubordinates == 1 && a.CountValidSubordinates == a.CountSubordinates && a.CountSubordinatesFailingToConnectToMain == a.CountSubordinates && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = DeadIntermediateMainWithSingleSubordinateFailingToConnect
			a.Description = "Intermediate main cannot be reached by orchestrator and its (single) subordinate is failing to connect"
			//
		} else /* intermediate-main */ if !a.IsMain && !a.LastCheckValid && a.CountSubordinates == 1 && a.CountValidSubordinates == a.CountSubordinates && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = DeadIntermediateMainWithSingleSubordinate
			a.Description = "Intermediate main cannot be reached by orchestrator and its (single) subordinate is not replicating"
			//
		} else /* intermediate-main */ if !a.IsMain && !a.LastCheckValid && a.CountSubordinates > 1 && a.CountValidSubordinates == a.CountSubordinates && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = DeadIntermediateMain
			a.Description = "Intermediate main cannot be reached by orchestrator and none of its subordinates is replicating"
			//
		} else if !a.IsMain && !a.LastCheckValid && a.CountValidSubordinates < a.CountSubordinates && a.CountValidSubordinates > 0 && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = DeadIntermediateMainAndSomeSubordinates
			a.Description = "Intermediate main cannot be reached by orchestrator; some of its subordinates are unreachable and none of its reachable subordinates is replicating"
			//
		} else if !a.IsMain && !a.LastCheckValid && a.CountValidSubordinates > 0 && a.CountValidReplicatingSubordinates > 0 {
			a.Analysis = UnreachableIntermediateMain
			a.Description = "Intermediate main cannot be reached by orchestrator but it has replicating subordinates; possibly a network/host issue"
			//
		} else if !a.IsMain && a.LastCheckValid && a.CountSubordinates > 1 && a.CountValidReplicatingSubordinates == 0 &&
			a.CountSubordinatesFailingToConnectToMain > 0 && a.CountSubordinatesFailingToConnectToMain == a.CountValidSubordinates {
			// All subordinates are either failing to connect to main (and at least one of these have to exist)
			// or completely dead.
			// Must have at least two subordinates to reach such conclusion -- do note that the intermediate main is still
			// reachable to orchestrator, so we base our conclusion on subordinates only at this point.
			a.Analysis = AllIntermediateMainSubordinatesFailingToConnectOrDead
			a.Description = "Intermediate main is reachable but all of its subordinates are failing to connect"
			//
		} else if !a.IsMain && a.LastCheckValid && a.CountSubordinates > 0 && a.CountValidReplicatingSubordinates == 0 {
			a.Analysis = AllIntermediateMainSubordinatesNotReplicating
			a.Description = "Intermediate main is reachable but none of its subordinates is replicating"
			//
		} else if a.IsBinlogServer && a.IsFailingToConnectToMain {
			a.Analysis = BinlogServerFailingToConnectToMain
			a.Description = "Binlog server is unable to connect to its main"
			//
		} else if a.ReplicationDepth == 1 && a.IsFailingToConnectToMain {
			a.Analysis = FirstTierSubordinateFailingToConnectToMain
			a.Description = "1st tier subordinate (directly replicating from topology main) is unable to connect to the main"
			//
		}
		//		 else if a.IsMain && a.CountSubordinates == 0 {
		//			a.Analysis = MainWithoutSubordinates
		//			a.Description = "Main has no subordinates"
		//		}

		appendAnalysis := func(analysis *ReplicationAnalysis) {
			if a.Analysis == NoProblem && len(a.StructureAnalysis) == 0 {
				return
			}
			skipThisHost := false
			for _, filter := range config.Config.RecoveryIgnoreHostnameFilters {
				if matched, _ := regexp.MatchString(filter, a.AnalyzedInstanceKey.Hostname); matched {
					skipThisHost = true
				}
			}
			if a.IsDowntimed && !includeDowntimed {
				skipThisHost = true
			}
			if !skipThisHost {
				result = append(result, a)
			}
		}

		{
			// Moving on to structure analysis
			// We also do structural checks. See if there's potential danger in promotions
			if a.IsMain && a.CountStatementBasedLoggingSubordinates > 0 && a.CountMixedBasedLoggingSubordinates > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndMixedLoggingSubordinatesStructureWarning)
			}
			if a.IsMain && a.CountStatementBasedLoggingSubordinates > 0 && a.CountRowBasedLoggingSubordinates > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, StatementAndRowLoggingSubordinatesStructureWarning)
			}
			if a.IsMain && a.CountMixedBasedLoggingSubordinates > 0 && a.CountRowBasedLoggingSubordinates > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, MixedAndRowLoggingSubordinatesStructureWarning)
			}
			if a.IsMain && a.CountDistinctMajorVersionsLoggingSubordinates > 1 {
				a.StructureAnalysis = append(a.StructureAnalysis, MultipleMajorVersionsLoggingSubordinates)
			}
		}
		appendAnalysis(&a)

		if a.CountSubordinates > 0 && auditAnalysis {
			// Interesting enough for analysis
			go auditInstanceAnalysisInChangelog(&a.AnalyzedInstanceKey, a.Analysis)
		}
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return result, err
}

// auditInstanceAnalysisInChangelog will write down an instance's analysis in the database_instance_analysis_changelog table.
// To not repeat recurring analysis code, the database_instance_last_analysis table is used, so that only changes to
// analysis codes are written.
func auditInstanceAnalysisInChangelog(instanceKey *InstanceKey, analysisCode AnalysisCode) error {
	if lastWrittenAnalysis, found := recentInstantAnalysis.Get(instanceKey.DisplayString()); found {
		if lastWrittenAnalysis == analysisCode {
			// Surely nothing new.
			// And let's expand the timeout
			recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
			return nil
		}
	}
	// Passed the cache; but does database agree that there's a change? Here's a persistent cache; this comes here
	// to verify no two orchestrator services are doing this without coordinating (namely, one dies, the other taking its place
	// and has no familiarity of the former's cache)
	analysisChangeWriteAttemptCounter.Inc(1)
	sqlResult, err := db.ExecOrchestrator(`
			insert ignore into database_instance_last_analysis (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				) on duplicate key update
					analysis = values(analysis),
					analysis_timestamp = if(analysis = values(analysis), analysis_timestamp, values(analysis_timestamp))
			`,
		instanceKey.Hostname, instanceKey.Port, string(analysisCode),
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	recentInstantAnalysis.Set(instanceKey.DisplayString(), analysisCode, cache.DefaultExpiration)
	lastAnalysisChanged := (rows > 0)

	if !lastAnalysisChanged {
		return nil
	}

	_, err = db.ExecOrchestrator(`
			insert into database_instance_analysis_changelog (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				)
			`,
		instanceKey.Hostname, instanceKey.Port, string(analysisCode),
	)
	if err == nil {
		analysisChangeWriteCounter.Inc(1)
	}
	return log.Errore(err)
}

// ExpireInstanceAnalysisChangelog removes old-enough analysis entries from the changelog
func ExpireInstanceAnalysisChangelog() error {
	_, err := db.ExecOrchestrator(`
			delete
				from database_instance_analysis_changelog
			where
				analysis_timestamp < now() - interval ? hour
			`,
		config.Config.UnseenInstanceForgetHours,
	)
	return log.Errore(err)
}

// ReadReplicationAnalysisChangelog
func ReadReplicationAnalysisChangelog() ([]ReplicationAnalysisChangelog, error) {
	res := []ReplicationAnalysisChangelog{}
	query := `
		select
            hostname,
            port,
			group_concat(analysis_timestamp,';',analysis order by changelog_id) as changelog
		from
			database_instance_analysis_changelog
		group by
			hostname, port
		`
	err := db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		analysisChangelog := ReplicationAnalysisChangelog{}

		analysisChangelog.AnalyzedInstanceKey.Hostname = m.GetString("hostname")
		analysisChangelog.AnalyzedInstanceKey.Port = m.GetInt("port")
		analysisChangelog.Changelog = m.GetString("changelog")

		res = append(res, analysisChangelog)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}
