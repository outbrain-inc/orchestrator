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
	"database/sql"
	"fmt"
	"time"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
	"github.com/outbrain/orchestrator/go/config"
	"github.com/outbrain/orchestrator/go/db"
)

// Max concurrency for bulk topology operations
const topologyConcurrency = 128

var topologyConcurrencyChan = make(chan bool, topologyConcurrency)

type OperationGTIDHint string

const (
	GTIDHintDeny    OperationGTIDHint = "NoGTID"
	GTIDHintNeutral                   = "GTIDHintNeutral"
	GTIDHintForce                     = "GTIDHintForce"
)

const sqlThreadPollDuration = 400 * time.Millisecond

// ExecInstance executes a given query on the given MySQL topology instance
func ExecInstance(instanceKey *InstanceKey, query string, args ...interface{}) (sql.Result, error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	res, err := sqlutils.Exec(db, query, args...)
	return res, err
}

// ExecInstanceNoPrepare executes a given query on the given MySQL topology instance, without using prepared statements
func ExecInstanceNoPrepare(instanceKey *InstanceKey, query string, args ...interface{}) (sql.Result, error) {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	res, err := sqlutils.ExecNoPrepare(db, query, args...)
	return res, err
}

// ExecuteOnTopology will execute given function while maintaining concurrency limit
// on topology servers. It is safe in the sense that we will not leak tokens.
func ExecuteOnTopology(f func()) {
	topologyConcurrencyChan <- true
	defer func() { recover(); <-topologyConcurrencyChan }()
	f()
}

// ScanInstanceRow executes a read-a-single-row query on a given MySQL topology instance
func ScanInstanceRow(instanceKey *InstanceKey, query string, dest ...interface{}) error {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	err = db.QueryRow(query).Scan(dest...)
	return err
}

// EmptyCommitInstance issues an empty COMMIT on a given instance
func EmptyCommitInstance(instanceKey *InstanceKey) error {
	db, err := db.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return err
}

// RefreshTopologyInstance will synchronuously re-read topology instance
func RefreshTopologyInstance(instanceKey *InstanceKey) (*Instance, error) {
	_, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return nil, err
	}

	inst, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return nil, err
	}

	return inst, nil
}

// RefreshTopologyInstances will do a blocking (though concurrent) refresh of all given instances
func RefreshTopologyInstances(instances [](*Instance)) {
	// use concurrency but wait for all to complete
	barrier := make(chan InstanceKey)
	for _, instance := range instances {
		instance := instance
		go func() {
			// Signal completed subordinate
			defer func() { barrier <- instance.Key }()
			// Wait your turn to read a subordinate
			ExecuteOnTopology(func() {
				log.Debugf("... reading instance: %+v", instance.Key)
				ReadTopologyInstanceUnbuffered(&instance.Key)
			})
		}()
	}
	for range instances {
		<-barrier
	}
}

// RefreshInstanceSubordinateHosts is a workaround for a bug in MySQL where
// SHOW SLAVE HOSTS continues to present old, long disconnected subordinates.
// It turns out issuing a couple FLUSH commands mitigates the problem.
func RefreshInstanceSubordinateHosts(instanceKey *InstanceKey) (*Instance, error) {
	_, _ = ExecInstance(instanceKey, `flush error logs`)
	_, _ = ExecInstance(instanceKey, `flush error logs`)

	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	return instance, err
}

// GetSubordinateRestartPreserveStatements returns a sequence of statements that make sure a subordinate is stopped
// and then returned to the same state. For example, if the subordinate was fully running, this will issue
// a STOP on both io_thread and sql_thread, followed by START on both. If one of them is not running
// at the time this function is called, said thread will be neither stopped nor started.
// The caller may provide an injected statememt, to be executed while the subordinate is stopped.
// This is useful for CHANGE MASTER TO commands, that unfortunately must take place while the subordinate
// is completely stopped.
func GetSubordinateRestartPreserveStatements(instanceKey *InstanceKey, injectedStatement string) (statements []string, err error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return statements, err
	}
	if instance.Subordinate_IO_Running {
		statements = append(statements, SemicolonTerminated(`stop subordinate io_thread`))
	}
	if instance.Subordinate_SQL_Running {
		statements = append(statements, SemicolonTerminated(`stop subordinate sql_thread`))
	}
	if injectedStatement != "" {
		statements = append(statements, SemicolonTerminated(injectedStatement))
	}
	if instance.Subordinate_SQL_Running {
		statements = append(statements, SemicolonTerminated(`start subordinate sql_thread`))
	}
	if instance.Subordinate_IO_Running {
		statements = append(statements, SemicolonTerminated(`start subordinate io_thread`))
	}
	return statements, err
}

// FlushBinaryLogs attempts a 'FLUSH BINARY LOGS' statement on the given instance.
func FlushBinaryLogs(instanceKey *InstanceKey, count int) (*Instance, error) {
	if *config.RuntimeCLIFlags.Noop {
		return nil, fmt.Errorf("noop: aborting flush-binary-logs operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	for i := 0; i < count; i++ {
		_, err := ExecInstance(instanceKey, `flush binary logs`)
		if err != nil {
			return nil, log.Errore(err)
		}
	}

	log.Infof("flush-binary-logs count=%+v on %+v", count, *instanceKey)
	AuditOperation("flush-binary-logs", instanceKey, "success")

	return ReadTopologyInstanceUnbuffered(instanceKey)
}

// FlushBinaryLogsTo attempts to 'FLUSH BINARY LOGS' until given binary log is reached
func FlushBinaryLogsTo(instanceKey *InstanceKey, logFile string) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	distance := instance.SelfBinlogCoordinates.FileNumberDistance(&BinlogCoordinates{LogFile: logFile})
	if distance < 0 {
		return nil, log.Errorf("FlushBinaryLogsTo: target log file %+v is smaller than current log file %+v", logFile, instance.SelfBinlogCoordinates.LogFile)
	}
	return FlushBinaryLogs(instanceKey, distance)
}

// FlushBinaryLogsTo attempts to 'PURGE BINARY LOGS' until given binary log is reached
func PurgeBinaryLogsTo(instanceKey *InstanceKey, logFile string) (*Instance, error) {
	if *config.RuntimeCLIFlags.Noop {
		return nil, fmt.Errorf("noop: aborting purge-binary-logs operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err := ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("purge binary logs to '%s'", logFile))
	if err != nil {
		return nil, log.Errore(err)
	}

	log.Infof("purge-binary-logs to=%+v on %+v", logFile, *instanceKey)
	AuditOperation("purge-binary-logs", instanceKey, "success")

	return ReadTopologyInstanceUnbuffered(instanceKey)
}

// FlushBinaryLogsTo attempts to 'PURGE BINARY LOGS' until given binary log is reached
func PurgeBinaryLogsToCurrent(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	return PurgeBinaryLogsTo(instanceKey, instance.SelfBinlogCoordinates.LogFile)
}

// StopSubordinateNicely stops a subordinate such that SQL_thread and IO_thread are aligned (i.e.
// SQL_thread consumes all relay log entries)
// It will actually START the sql_thread even if the subordinate is completely stopped.
func StopSubordinateNicely(instanceKey *InstanceKey, timeout time.Duration) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSubordinate() {
		return instance, fmt.Errorf("instance is not a subordinate: %+v", instanceKey)
	}

	_, err = ExecInstanceNoPrepare(instanceKey, `stop subordinate io_thread`)
	_, err = ExecInstanceNoPrepare(instanceKey, `start subordinate sql_thread`)

	if instance.SQLDelay == 0 {
		// Otherwise we don't bother.
		startTime := time.Now()
		for upToDate := false; !upToDate; {
			if timeout > 0 && time.Since(startTime) >= timeout {
				// timeout
				return nil, log.Errorf("StopSubordinateNicely timeout on %+v", *instanceKey)
			}
			instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
			if err != nil {
				return instance, log.Errore(err)
			}

			if instance.SQLThreadUpToDate() {
				upToDate = true
			} else {
				time.Sleep(sqlThreadPollDuration)
			}
		}
	}
	_, err = ExecInstanceNoPrepare(instanceKey, `stop subordinate`)
	if err != nil {
		// Patch; current MaxScale behavior for STOP SLAVE is to throw an error if subordinate already stopped.
		if instance.isMaxScale() && err.Error() == "Error 1199: Subordinate connection is not running" {
			err = nil
		}
	}
	if err != nil {
		return instance, log.Errore(err)
	}

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	log.Infof("Stopped subordinate nicely on %+v, Self:%+v, Exec:%+v", *instanceKey, instance.SelfBinlogCoordinates, instance.ExecBinlogCoordinates)
	return instance, err
}

// StopSubordinatesNicely will attemt to stop all given subordinates nicely, up to timeout
func StopSubordinatesNicely(subordinates [](*Instance), timeout time.Duration) [](*Instance) {
	refreshedSubordinates := [](*Instance){}

	log.Debugf("Stopping %d subordinates nicely", len(subordinates))
	// use concurrency but wait for all to complete
	barrier := make(chan *Instance)
	for _, subordinate := range subordinates {
		subordinate := subordinate
		go func() {
			updatedSubordinate := &subordinate
			// Signal completed subordinate
			defer func() { barrier <- *updatedSubordinate }()
			// Wait your turn to read a subordinate
			ExecuteOnTopology(func() {
				StopSubordinateNicely(&subordinate.Key, timeout)
				subordinate, _ = StopSubordinate(&subordinate.Key)
				updatedSubordinate = &subordinate
			})
		}()
	}
	for range subordinates {
		refreshedSubordinates = append(refreshedSubordinates, <-barrier)
	}
	return refreshedSubordinates
}

// StopSubordinate stops replication on a given instance
func StopSubordinate(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSubordinate() {
		return instance, fmt.Errorf("instance is not a subordinate: %+v", instanceKey)
	}
	_, err = ExecInstanceNoPrepare(instanceKey, `stop subordinate`)
	if err != nil {
		// Patch; current MaxScale behavior for STOP SLAVE is to throw an error if subordinate already stopped.
		if instance.isMaxScale() && err.Error() == "Error 1199: Subordinate connection is not running" {
			err = nil
		}
	}
	if err != nil {

		return instance, log.Errore(err)
	}
	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)

	log.Infof("Stopped subordinate on %+v, Self:%+v, Exec:%+v", *instanceKey, instance.SelfBinlogCoordinates, instance.ExecBinlogCoordinates)
	return instance, err
}

// StartSubordinate starts replication on a given instance.
func StartSubordinate(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSubordinate() {
		return instance, fmt.Errorf("instance is not a subordinate: %+v", instanceKey)
	}

	// If async fallback is disallowed, we'd better make sure to enable subordinates to
	// send ACKs before START SLAVE. Subordinate ACKing is off at mysqld startup because
	// some subordinates (those that must never be promoted) should never ACK.
	// Note: We assume that subordinates use 'skip-subordinate-start' so they won't
	//       START SLAVE on their own upon restart.
	if instance.SemiSyncEnforced {
		// Send ACK only from promotable instances.
		sendACK := instance.PromotionRule != MustNotPromoteRule
		// Always disable main setting, in case we're converting a former main.
		if err := EnableSemiSync(instanceKey, false, sendACK); err != nil {
			return instance, log.Errore(err)
		}
	}

	_, err = ExecInstanceNoPrepare(instanceKey, `start subordinate`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Started subordinate on %+v", instanceKey)
	if config.Config.SubordinateStartPostWaitMilliseconds > 0 {
		time.Sleep(time.Duration(config.Config.SubordinateStartPostWaitMilliseconds) * time.Millisecond)
	}

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	return instance, err
}

// RestartSubordinate stops & starts replication on a given instance
func RestartSubordinate(instanceKey *InstanceKey) (instance *Instance, err error) {
	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	instance, err = StartSubordinate(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	return instance, nil

}

// StartSubordinates will do concurrent start-subordinate
func StartSubordinates(subordinates [](*Instance)) {
	// use concurrency but wait for all to complete
	log.Debugf("Starting %d subordinates", len(subordinates))
	barrier := make(chan InstanceKey)
	for _, instance := range subordinates {
		instance := instance
		go func() {
			// Signal compelted subordinate
			defer func() { barrier <- instance.Key }()
			// Wait your turn to read a subordinate
			ExecuteOnTopology(func() { StartSubordinate(&instance.Key) })
		}()
	}
	for range subordinates {
		<-barrier
	}
}

// StartSubordinateUntilMainCoordinates issuesa START SLAVE UNTIL... statement on given instance
func StartSubordinateUntilMainCoordinates(instanceKey *InstanceKey, mainCoordinates *BinlogCoordinates) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSubordinate() {
		return instance, fmt.Errorf("instance is not a subordinate: %+v", instanceKey)
	}
	if instance.SubordinateRunning() {
		return instance, fmt.Errorf("subordinate already running: %+v", instanceKey)
	}

	log.Infof("Will start subordinate on %+v until coordinates: %+v", instanceKey, mainCoordinates)

	if instance.SemiSyncEnforced {
		// Send ACK only from promotable instances.
		sendACK := instance.PromotionRule != MustNotPromoteRule
		// Always disable main setting, in case we're converting a former main.
		if err := EnableSemiSync(instanceKey, false, sendACK); err != nil {
			return instance, log.Errore(err)
		}
	}

	// MariaDB has a bug: a CHANGE MASTER TO statement does not work properly with prepared statement... :P
	// See https://mariadb.atlassian.net/browse/MDEV-7640
	// This is the reason for ExecInstanceNoPrepare
	_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("start subordinate until main_log_file='%s', main_log_pos=%d",
		mainCoordinates.LogFile, mainCoordinates.LogPos))
	if err != nil {
		return instance, log.Errore(err)
	}

	for upToDate := false; !upToDate; {
		instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
		if err != nil {
			return instance, log.Errore(err)
		}

		switch {
		case instance.ExecBinlogCoordinates.SmallerThan(mainCoordinates):
			time.Sleep(sqlThreadPollDuration)
		case instance.ExecBinlogCoordinates.Equals(mainCoordinates):
			upToDate = true
		case mainCoordinates.SmallerThan(&instance.ExecBinlogCoordinates):
			return instance, fmt.Errorf("Start SLAVE UNTIL is past coordinates: %+v", instanceKey)
		}
	}

	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	return instance, err
}

// EnableSemiSync sets the rpl_semi_sync_(main|subordinate)_enabled variables
// on a given instance.
func EnableSemiSync(instanceKey *InstanceKey, main, subordinate bool) error {
	log.Infof("instance %+v rpl_semi_sync_main_enabled: %t, rpl_semi_sync_subordinate_enabled: %t", instanceKey, main, subordinate)
	_, err := ExecInstanceNoPrepare(instanceKey,
		`set global rpl_semi_sync_main_enabled = ?, global rpl_semi_sync_subordinate_enabled = ?`,
		main, subordinate)
	return err
}

// ChangeMainCredentials issues a CHANGE MASTER TO... MASTER_USER=, MASTER_PASSWORD=...
func ChangeMainCredentials(instanceKey *InstanceKey, mainUser string, mainPassword string) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	if mainUser == "" {
		return instance, log.Errorf("Empty user in ChangeMainCredentials() for %+v", *instanceKey)
	}

	if instance.SubordinateRunning() {
		return instance, fmt.Errorf("ChangeMainTo: Cannot change main on: %+v because subordinate is running", *instanceKey)
	}
	log.Debugf("ChangeMainTo: will attempt changing main credentials on %+v", *instanceKey)

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting CHANGE MASTER TO operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}
	_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change main to main_user='%s', main_password='%s'",
		mainUser, mainPassword))

	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("ChangeMainTo: Changed main credentials on %+v", *instanceKey)

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	return instance, err
}

// ChangeMainTo changes the given instance's main according to given input.
func ChangeMainTo(instanceKey *InstanceKey, mainKey *InstanceKey, mainBinlogCoordinates *BinlogCoordinates, skipUnresolve bool, gtidHint OperationGTIDHint) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SubordinateRunning() {
		return instance, fmt.Errorf("ChangeMainTo: Cannot change main on: %+v because subordinate is running", *instanceKey)
	}
	log.Debugf("ChangeMainTo: will attempt changing main on %+v to %+v, %+v", *instanceKey, *mainKey, *mainBinlogCoordinates)
	changeToMainKey := mainKey
	if !skipUnresolve {
		unresolvedMainKey, nameUnresolved, err := UnresolveHostname(mainKey)
		if err != nil {
			log.Debugf("ChangeMainTo: aborting operation on %+v due to resolving error on %+v: %+v", *instanceKey, *mainKey, err)
			return instance, err
		}
		if nameUnresolved {
			log.Debugf("ChangeMainTo: Unresolved %+v into %+v", *mainKey, unresolvedMainKey)
		}
		changeToMainKey = &unresolvedMainKey
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting CHANGE MASTER TO operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	originalMainKey := instance.MainKey
	originalExecBinlogCoordinates := instance.ExecBinlogCoordinates

	changedViaGTID := false
	if instance.UsingMariaDBGTID && gtidHint != GTIDHintDeny {
		// MariaDB has a bug: a CHANGE MASTER TO statement does not work properly with prepared statement... :P
		// See https://mariadb.atlassian.net/browse/MDEV-7640
		// This is the reason for ExecInstanceNoPrepare
		// Keep on using GTID
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change main to main_host='%s', main_port=%d",
			changeToMainKey.Hostname, changeToMainKey.Port))
		changedViaGTID = true
	} else if instance.UsingMariaDBGTID && gtidHint == GTIDHintDeny {
		// Make sure to not use GTID
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change main to main_host='%s', main_port=%d, main_log_file='%s', main_log_pos=%d, main_use_gtid=no",
			changeToMainKey.Hostname, changeToMainKey.Port, mainBinlogCoordinates.LogFile, mainBinlogCoordinates.LogPos))
	} else if instance.IsMariaDB() && gtidHint == GTIDHintForce {
		// Is MariaDB; not using GTID, turn into GTID
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change main to main_host='%s', main_port=%d, main_use_gtid=subordinate_pos",
			changeToMainKey.Hostname, changeToMainKey.Port))
		changedViaGTID = true
	} else if instance.UsingOracleGTID && gtidHint != GTIDHintDeny {
		// Is Oracle; already uses GTID; keep using it.
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change main to main_host='%s', main_port=%d",
			changeToMainKey.Hostname, changeToMainKey.Port))
		changedViaGTID = true
	} else if instance.UsingOracleGTID && gtidHint == GTIDHintDeny {
		// Is Oracle; already uses GTID
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change main to main_host='%s', main_port=%d, main_log_file='%s', main_log_pos=%d, main_auto_position=0",
			changeToMainKey.Hostname, changeToMainKey.Port, mainBinlogCoordinates.LogFile, mainBinlogCoordinates.LogPos))
	} else if instance.SupportsOracleGTID && gtidHint == GTIDHintForce {
		// Is Oracle; not using GTID right now; turn into GTID
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change main to main_host='%s', main_port=%d, main_auto_position=1",
			changeToMainKey.Hostname, changeToMainKey.Port))
		changedViaGTID = true
	} else {
		// Normal binlog file:pos
		_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf("change main to main_host='%s', main_port=%d, main_log_file='%s', main_log_pos=%d",
			changeToMainKey.Hostname, changeToMainKey.Port, mainBinlogCoordinates.LogFile, mainBinlogCoordinates.LogPos))
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	WriteMainPositionEquivalence(&originalMainKey, &originalExecBinlogCoordinates, changeToMainKey, mainBinlogCoordinates)

	log.Infof("ChangeMainTo: Changed main on %+v to: %+v, %+v. GTID: %+v", *instanceKey, mainKey, mainBinlogCoordinates, changedViaGTID)

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	return instance, err
}

// SkipToNextBinaryLog changes main position to beginning of next binlog
// USE WITH CARE!
// Use case is binlog servers where the main was gone & replaced by another.
func SkipToNextBinaryLog(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	nextFileCoordinates, err := instance.ExecBinlogCoordinates.NextFileCoordinates()
	if err != nil {
		return instance, log.Errore(err)
	}
	nextFileCoordinates.LogPos = 4
	log.Debugf("Will skip replication on %+v to next binary log: %+v", instance.Key, nextFileCoordinates.LogFile)

	instance, err = ChangeMainTo(&instance.Key, &instance.MainKey, &nextFileCoordinates, false, GTIDHintNeutral)
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("skip-binlog", instanceKey, fmt.Sprintf("Skipped replication to next binary log: %+v", nextFileCoordinates.LogFile))
	return StartSubordinate(instanceKey)
}

// ResetSubordinate resets a subordinate, breaking the replication
func ResetSubordinate(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SubordinateRunning() {
		return instance, fmt.Errorf("Cannot reset subordinate on: %+v because subordinate is running", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reset-subordinate operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	// MySQL's RESET SLAVE is done correctly; however SHOW SLAVE STATUS still returns old hostnames etc
	// and only resets till after next restart. This leads to orchestrator still thinking the instance replicates
	// from old host. We therefore forcibly modify the hostname.
	// RESET SLAVE ALL command solves this, but only as of 5.6.3
	_, err = ExecInstanceNoPrepare(instanceKey, `change main to main_host='_'`)
	if err != nil {
		return instance, log.Errore(err)
	}
	_, err = ExecInstanceNoPrepare(instanceKey, `reset subordinate /*!50603 all */`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Reset subordinate %+v", instanceKey)

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	return instance, err
}

// ResetMain issues a RESET MASTER statement on given instance. Use with extreme care!
func ResetMain(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SubordinateRunning() {
		return instance, fmt.Errorf("Cannot reset main on: %+v because subordinate is running", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reset-main operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstanceNoPrepare(instanceKey, `reset main`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Reset main %+v", instanceKey)

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	return instance, err
}

// skipQueryClassic skips a query in normal binlog file:pos replication
func setGTIDPurged(instance *Instance, gtidPurged string) error {
	if *config.RuntimeCLIFlags.Noop {
		return fmt.Errorf("noop: aborting set-gtid-purged operation on %+v; signalling error but nothing went wrong.", instance.Key)
	}

	_, err := ExecInstance(&instance.Key, fmt.Sprintf(`set global gtid_purged := '%s'`, gtidPurged))
	return err
}

// skipQueryClassic skips a query in normal binlog file:pos replication
func skipQueryClassic(instance *Instance) error {
	_, err := ExecInstance(&instance.Key, `set global sql_subordinate_skip_counter := 1`)
	return err
}

// skipQueryOracleGtid skips a single query in an Oracle GTID replicating subordinate, by injecting an empty transaction
func skipQueryOracleGtid(instance *Instance) error {
	nextGtid, err := instance.NextGTID()
	if err != nil {
		return err
	}
	if nextGtid == "" {
		return fmt.Errorf("Empty NextGTID() in skipQueryGtid() for %+v", instance.Key)
	}
	if _, err := ExecInstanceNoPrepare(&instance.Key, fmt.Sprintf(`SET GTID_NEXT='%s'`, nextGtid)); err != nil {
		return err
	}
	if err := EmptyCommitInstance(&instance.Key); err != nil {
		return err
	}
	if _, err := ExecInstanceNoPrepare(&instance.Key, `SET GTID_NEXT='AUTOMATIC'`); err != nil {
		return err
	}
	return nil
}

// SkipQuery skip a single query in a failed replication instance
func SkipQuery(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsSubordinate() {
		return instance, fmt.Errorf("instance is not a subordinate: %+v", instanceKey)
	}
	if instance.Subordinate_SQL_Running {
		return instance, fmt.Errorf("Subordinate SQL thread is running on %+v", instanceKey)
	}
	if instance.LastSQLError == "" {
		return instance, fmt.Errorf("No SQL error on %+v", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting skip-query operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	log.Debugf("Skipping one query on %+v", instanceKey)
	if instance.UsingOracleGTID {
		err = skipQueryOracleGtid(instance)
	} else if instance.UsingMariaDBGTID {
		return instance, log.Errorf("%+v is replicating with MariaDB GTID. To skip a query first disable GTID, then skip, then enable GTID again", *instanceKey)
	} else {
		err = skipQueryClassic(instance)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	AuditOperation("skip-query", instanceKey, "Skipped one query")
	return StartSubordinate(instanceKey)
}

// DetachSubordinate detaches a subordinate from replication; forcibly corrupting the binlog coordinates (though in such way
// that is reversible)
func DetachSubordinate(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SubordinateRunning() {
		return instance, fmt.Errorf("Cannot detach subordinate on: %+v because subordinate is running", instanceKey)
	}

	isDetached, _, _ := instance.ExecBinlogCoordinates.DetachedCoordinates()

	if isDetached {
		return instance, fmt.Errorf("Cannot (need not) detach subordinate on: %+v because subordinate is already detached", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting detach-subordinate operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	detachedCoordinates := BinlogCoordinates{LogFile: fmt.Sprintf("//%s:%d", instance.ExecBinlogCoordinates.LogFile, instance.ExecBinlogCoordinates.LogPos), LogPos: instance.ExecBinlogCoordinates.LogPos}
	// Encode the current coordinates within the log file name, in such way that replication is broken, but info can still be resurrected
	_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf(`change main to main_log_file='%s', main_log_pos=%d`, detachedCoordinates.LogFile, detachedCoordinates.LogPos))
	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("Detach subordinate %+v", instanceKey)

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	return instance, err
}

// ReattachSubordinate restores a detached subordinate back into replication
func ReattachSubordinate(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.SubordinateRunning() {
		return instance, fmt.Errorf("Cannot (need not) reattach subordinate on: %+v because subordinate is running", instanceKey)
	}

	isDetached, detachedLogFile, detachedLogPos := instance.ExecBinlogCoordinates.DetachedCoordinates()

	if !isDetached {
		return instance, fmt.Errorf("Cannot reattach subordinate on: %+v because subordinate is not detached", instanceKey)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reattach-subordinate operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstanceNoPrepare(instanceKey, fmt.Sprintf(`change main to main_log_file='%s', main_log_pos=%s`, detachedLogFile, detachedLogPos))
	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("Reattach subordinate %+v", instanceKey)

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	return instance, err
}

// MainPosWait issues a MASTER_POS_WAIT() an given instance according to given coordinates.
func MainPosWait(instanceKey *InstanceKey, binlogCoordinates *BinlogCoordinates) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	_, err = ExecInstance(instanceKey, `select main_pos_wait(?, ?)`, binlogCoordinates.LogFile, binlogCoordinates.LogPos)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Instance %+v has reached coordinates: %+v", instanceKey, binlogCoordinates)

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	return instance, err
}

// Attempt to read and return replication credentials from the mysql.subordinate_main_info system table
func ReadReplicationCredentials(instanceKey *InstanceKey) (replicationUser string, replicationPassword string, err error) {
	query := `
		select
			ifnull(max(User_name), '') as user,
			ifnull(max(User_password), '') as password
		from
			mysql.subordinate_main_info
	`
	err = ScanInstanceRow(instanceKey, query, &replicationUser, &replicationPassword)
	if err != nil {
		return replicationUser, replicationPassword, err
	}
	if replicationUser == "" {
		err = fmt.Errorf("Cannot find credentials in mysql.subordinate_main_info")
	}
	return replicationUser, replicationPassword, err
}

// SetReadOnly sets or clears the instance's global read_only variable
func SetReadOnly(instanceKey *InstanceKey, readOnly bool) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting set-read-only operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	// If async fallback is disallowed, we're responsible for flipping the main
	// semi-sync switch ON before accepting writes. The setting is off by default.
	if instance.SemiSyncEnforced && !readOnly {
		// Send ACK only from promotable instances.
		sendACK := instance.PromotionRule != MustNotPromoteRule
		if err := EnableSemiSync(instanceKey, true, sendACK); err != nil {
			return instance, log.Errore(err)
		}
	}

	_, err = ExecInstance(instanceKey, fmt.Sprintf("set global read_only = %t", readOnly))
	if err != nil {
		return instance, log.Errore(err)
	}
	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)

	// If we just went read-only, it's safe to flip the main semi-sync switch
	// OFF, which is the default value so that subordinates can make progress.
	if instance.SemiSyncEnforced && readOnly {
		// Send ACK only from promotable instances.
		sendACK := instance.PromotionRule != MustNotPromoteRule
		if err := EnableSemiSync(instanceKey, false, sendACK); err != nil {
			return instance, log.Errore(err)
		}
	}

	log.Infof("instance %+v read_only: %t", instanceKey, readOnly)
	AuditOperation("read-only", instanceKey, fmt.Sprintf("set as %t", readOnly))

	return instance, err
}

// KillQuery stops replication on a given instance
func KillQuery(instanceKey *InstanceKey, process int64) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	if *config.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting kill-query operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecInstance(instanceKey, fmt.Sprintf(`kill query %d`, process))
	if err != nil {
		return instance, log.Errore(err)
	}

	instance, err = ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("Killed query on %+v", *instanceKey)
	AuditOperation("kill-query", instanceKey, fmt.Sprintf("Killed query %d", process))
	return instance, err
}
