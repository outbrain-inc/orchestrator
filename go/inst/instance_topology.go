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

package inst

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/math"
	"github.com/outbrain/orchestrator/go/config"
)

// getASCIITopologyEntry will get an ascii topology tree rooted at given
// instance. It recursively draws the tree.
func getASCIITopologyEntry(depth int, instance *Instance, replicationMap map[*Instance]([]*Instance), extendedOutput bool) []string {
	if instance == nil {
		return []string{}
	}
	if instance.IsCoMain && depth > 1 {
		return []string{}
	}
	prefix := ""
	if depth > 0 {
		prefix = strings.Repeat(" ", (depth-1)*2)
		if instance.SubordinateRunning() && instance.IsLastCheckValid && instance.IsRecentlyChecked {
			prefix += "+ "
		} else {
			prefix += "- "
		}
	}
	entry := fmt.Sprintf("%s%s", prefix, instance.Key.DisplayString())
	if extendedOutput {
		entry = fmt.Sprintf("%s %s", entry, instance.HumanReadableDescription())
	}
	result := []string{entry}
	for _, subordinate := range replicationMap[instance] {
		subordinatesResult := getASCIITopologyEntry(depth+1, subordinate, replicationMap, extendedOutput)
		result = append(result, subordinatesResult...)
	}
	return result
}

// ASCIITopology returns a string representation of the topology of given cluster.
func ASCIITopology(clusterName string, historyTimestampPattern string) (result string, err error) {
	var instances [](*Instance)
	if historyTimestampPattern == "" {
		instances, err = ReadClusterInstances(clusterName)
	} else {
		instances, err = ReadHistoryClusterInstances(clusterName, historyTimestampPattern)
	}
	if err != nil {
		return "", err
	}

	instancesMap := make(map[InstanceKey](*Instance))
	for _, instance := range instances {
		log.Debugf("instanceKey: %+v", instance.Key)
		instancesMap[instance.Key] = instance
	}

	replicationMap := make(map[*Instance]([]*Instance))
	var mainInstance *Instance
	// Investigate subordinates:
	for _, instance := range instances {
		main, ok := instancesMap[instance.MainKey]
		if ok {
			if _, ok := replicationMap[main]; !ok {
				replicationMap[main] = [](*Instance){}
			}
			replicationMap[main] = append(replicationMap[main], instance)
		} else {
			mainInstance = instance
		}
	}
	// Get entries:
	var entries []string
	if mainInstance != nil {
		// Single main
		entries = getASCIITopologyEntry(0, mainInstance, replicationMap, historyTimestampPattern == "")
	} else {
		// Co-mains? For visualization we put each in its own branch while ignoring its other co-mains.
		for _, instance := range instances {
			if instance.IsCoMain {
				entries = append(entries, getASCIITopologyEntry(1, instance, replicationMap, historyTimestampPattern == "")...)
			}
		}
	}
	// Beautify: make sure the "[...]" part is nicely aligned for all instances.
	{
		maxIndent := 0
		for _, entry := range entries {
			maxIndent = math.MaxInt(maxIndent, strings.Index(entry, "["))
		}
		for i, entry := range entries {
			entryIndent := strings.Index(entry, "[")
			if maxIndent > entryIndent {
				tokens := strings.Split(entry, "[")
				newEntry := fmt.Sprintf("%s%s[%s", tokens[0], strings.Repeat(" ", maxIndent-entryIndent), tokens[1])
				entries[i] = newEntry
			}
		}
	}
	// Turn into string
	result = strings.Join(entries, "\n")
	return result, nil
}

// GetInstanceMain synchronously reaches into the replication topology
// and retrieves main's data
func GetInstanceMain(instance *Instance) (*Instance, error) {
	main, err := ReadTopologyInstanceUnbuffered(&instance.MainKey)
	return main, err
}

// InstancesAreSiblings checks whether both instances are replicating from same main
func InstancesAreSiblings(instance0, instance1 *Instance) bool {
	if !instance0.IsSubordinate() {
		return false
	}
	if !instance1.IsSubordinate() {
		return false
	}
	if instance0.Key.Equals(&instance1.Key) {
		// same instance...
		return false
	}
	return instance0.MainKey.Equals(&instance1.MainKey)
}

// InstanceIsMainOf checks whether an instance is the main of another
func InstanceIsMainOf(allegedMain, allegedSubordinate *Instance) bool {
	if !allegedSubordinate.IsSubordinate() {
		return false
	}
	if allegedMain.Key.Equals(&allegedSubordinate.Key) {
		// same instance...
		return false
	}
	return allegedMain.Key.Equals(&allegedSubordinate.MainKey)
}

// MoveEquivalent will attempt moving instance indicated by instanceKey below another instance,
// based on known main coordinates equivalence
func MoveEquivalent(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return instance, err
	}
	if instance.Key.Equals(otherKey) {
		return instance, fmt.Errorf("MoveEquivalent: attempt to move an instance below itself %+v", instance.Key)
	}

	// Are there equivalent coordinates to this instance?
	instanceCoordinates := &InstanceBinlogCoordinates{Key: instance.MainKey, Coordinates: instance.ExecBinlogCoordinates}
	binlogCoordinates, err := GetEquivalentBinlogCoordinatesFor(instanceCoordinates, otherKey)
	if err != nil {
		return instance, err
	}
	if binlogCoordinates == nil {
		return instance, fmt.Errorf("No equivalent coordinates found for %+v replicating from %+v at %+v", instance.Key, instance.MainKey, instance.ExecBinlogCoordinates)
	}
	// For performance reasons, we did all the above before even checking the subordinate is stopped or stopping it at all.
	// This allows us to quickly skip the entire operation should there NOT be coordinates.
	// To elaborate: if the subordinate is actually running AND making progress, it is unlikely/impossible for it to have
	// equivalent coordinates, as the current coordinates are like to have never been seen.
	// This excludes the case, for example, that the main is itself not replicating.
	// Now if we DO get to happen on equivalent coordinates, we need to double check. For CHANGE MASTER to happen we must
	// stop the subordinate anyhow. But then let's verify the position hasn't changed.
	knownExecBinlogCoordinates := instance.ExecBinlogCoordinates
	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}
	if !instance.ExecBinlogCoordinates.Equals(&knownExecBinlogCoordinates) {
		// Seems like things were still running... We don't have an equivalence point
		err = fmt.Errorf("MoveEquivalent(): ExecBinlogCoordinates changed after stopping replication on %+v; aborting", instance.Key)
		goto Cleanup
	}
	instance, err = ChangeMainTo(instanceKey, otherKey, binlogCoordinates, false, GTIDHintNeutral)

Cleanup:
	instance, _ = StartSubordinate(instanceKey)

	if err == nil {
		message := fmt.Sprintf("moved %+v via equivalence coordinates below %+v", *instanceKey, *otherKey)
		log.Debugf(message)
		AuditOperation("move-equivalent", instanceKey, message)
	}
	return instance, err
}

// MoveUp will attempt moving instance indicated by instanceKey up the topology hierarchy.
// It will perform all safety and sanity checks and will tamper with this instance's replication
// as well as its main.
func MoveUp(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsSubordinate() {
		return instance, fmt.Errorf("instance is not a subordinate: %+v", instanceKey)
	}
	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}
	main, err := GetInstanceMain(instance)
	if err != nil {
		return instance, log.Errorf("Cannot GetInstanceMain() for %+v. error=%+v", instance.Key, err)
	}

	if !main.IsSubordinate() {
		return instance, fmt.Errorf("main is not a subordinate itself: %+v", main.Key)
	}

	if canReplicate, err := instance.CanReplicateFrom(main); canReplicate == false {
		return instance, err
	}
	if main.IsBinlogServer() {
		// Quick solution via binlog servers
		return Repoint(instanceKey, &main.MainKey, GTIDHintDeny)
	}

	log.Infof("Will move %+v up the topology", *instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "move up"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := BeginMaintenance(&main.Key, GetMaintenanceOwner(), fmt.Sprintf("child %+v moves up", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", main.Key)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if !instance.UsingMariaDBGTID {
		main, err = StopSubordinate(&main.Key)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

	if !instance.UsingMariaDBGTID {
		instance, err = StartSubordinateUntilMainCoordinates(instanceKey, &main.SelfBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}

	// We can skip hostname unresolve; we just copy+paste whatever our main thinks of its main.
	instance, err = ChangeMainTo(instanceKey, &main.MainKey, &main.ExecBinlogCoordinates, true, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)
	if !instance.UsingMariaDBGTID {
		main, _ = StartSubordinate(&main.Key)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("move-up", instanceKey, fmt.Sprintf("moved up %+v. Previous main: %+v", *instanceKey, main.Key))

	return instance, err
}

// MoveUpSubordinates will attempt moving up all subordinates of a given instance, at the same time.
// Clock-time, this is fater than moving one at a time. However this means all subordinates of the given instance, and the instance itself,
// will all stop replicating together.
func MoveUpSubordinates(instanceKey *InstanceKey, pattern string) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}
	subordinateMutex := make(chan bool, 1)
	var barrier chan *InstanceKey

	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return res, nil, err, errs
	}
	if !instance.IsSubordinate() {
		return res, instance, fmt.Errorf("instance is not a subordinate: %+v", instanceKey), errs
	}
	_, err = GetInstanceMain(instance)
	if err != nil {
		return res, instance, log.Errorf("Cannot GetInstanceMain() for %+v. error=%+v", instance.Key, err), errs
	}

	if instance.IsBinlogServer() {
		subordinates, err, errors := RepointSubordinatesTo(instanceKey, pattern, &instance.MainKey)
		// Bail out!
		return subordinates, instance, err, errors
	}

	subordinates, err := ReadSubordinateInstances(instanceKey)
	if err != nil {
		return res, instance, err, errs
	}
	subordinates = filterInstancesByPattern(subordinates, pattern)
	if len(subordinates) == 0 {
		return res, instance, nil, errs
	}
	log.Infof("Will move subordinates of %+v up the topology", *instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "move up subordinates"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	for _, subordinate := range subordinates {
		if maintenanceToken, merr := BeginMaintenance(&subordinate.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v moves up", subordinate.Key)); merr != nil {
			err = fmt.Errorf("Cannot begin maintenance on %+v", subordinate.Key)
			goto Cleanup
		} else {
			defer EndMaintenance(maintenanceToken)
		}
	}

	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

	barrier = make(chan *InstanceKey)
	for _, subordinate := range subordinates {
		subordinate := subordinate
		go func() {
			defer func() {
				defer func() { barrier <- &subordinate.Key }()
				StartSubordinate(&subordinate.Key)
			}()

			var subordinateErr error
			ExecuteOnTopology(func() {
				if canReplicate, err := subordinate.CanReplicateFrom(instance); canReplicate == false || err != nil {
					subordinateErr = err
					return
				}
				if instance.IsBinlogServer() {
					// Special case. Just repoint
					subordinate, err = Repoint(&subordinate.Key, instanceKey, GTIDHintDeny)
					if err != nil {
						subordinateErr = err
						return
					}
				} else {
					// Normal case. Do the math.
					subordinate, err = StopSubordinate(&subordinate.Key)
					if err != nil {
						subordinateErr = err
						return
					}
					subordinate, err = StartSubordinateUntilMainCoordinates(&subordinate.Key, &instance.SelfBinlogCoordinates)
					if err != nil {
						subordinateErr = err
						return
					}

					subordinate, err = ChangeMainTo(&subordinate.Key, &instance.MainKey, &instance.ExecBinlogCoordinates, false, GTIDHintDeny)
					if err != nil {
						subordinateErr = err
						return
					}
				}
			})

			func() {
				subordinateMutex <- true
				defer func() { <-subordinateMutex }()
				if subordinateErr == nil {
					res = append(res, subordinate)
				} else {
					errs = append(errs, subordinateErr)
				}
			}()
		}()
	}
	for range subordinates {
		<-barrier
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)
	if err != nil {
		return res, instance, log.Errore(err), errs
	}
	if len(errs) == len(subordinates) {
		// All returned with error
		return res, instance, log.Error("Error on all operations"), errs
	}
	AuditOperation("move-up-subordinates", instanceKey, fmt.Sprintf("moved up %d/%d subordinates of %+v. New main: %+v", len(res), len(subordinates), *instanceKey, instance.MainKey))

	return res, instance, err, errs
}

// MoveBelow will attempt moving instance indicated by instanceKey below its supposed sibling indicated by sinblingKey.
// It will perform all safety and sanity checks and will tamper with this instance's replication
// as well as its sibling.
func MoveBelow(instanceKey, siblingKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	sibling, err := ReadTopologyInstanceUnbuffered(siblingKey)
	if err != nil {
		return instance, err
	}

	if sibling.IsBinlogServer() {
		// Binlog server has same coordinates as main
		// Easy solution!
		return Repoint(instanceKey, &sibling.Key, GTIDHintDeny)
	}

	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}

	rinstance, _, _ = ReadInstance(&sibling.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}
	if !InstancesAreSiblings(instance, sibling) {
		return instance, fmt.Errorf("instances are not siblings: %+v, %+v", *instanceKey, *siblingKey)
	}

	if canReplicate, err := instance.CanReplicateFrom(sibling); !canReplicate {
		return instance, err
	}
	log.Infof("Will move %+v below %+v", instanceKey, siblingKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("move below %+v", *siblingKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := BeginMaintenance(siblingKey, GetMaintenanceOwner(), fmt.Sprintf("%+v moves below this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *siblingKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

	sibling, err = StopSubordinate(siblingKey)
	if err != nil {
		goto Cleanup
	}
	if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
		instance, err = StartSubordinateUntilMainCoordinates(instanceKey, &sibling.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	} else if sibling.ExecBinlogCoordinates.SmallerThan(&instance.ExecBinlogCoordinates) {
		sibling, err = StartSubordinateUntilMainCoordinates(siblingKey, &instance.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}
	// At this point both siblings have executed exact same statements and are identical

	instance, err = ChangeMainTo(instanceKey, &sibling.Key, &sibling.SelfBinlogCoordinates, false, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)
	sibling, _ = StartSubordinate(siblingKey)

	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("move-below", instanceKey, fmt.Sprintf("moved %+v below %+v", *instanceKey, *siblingKey))

	return instance, err
}

func canMoveViaGTID(instance, otherInstance *Instance) (isOracleGTID bool, isMariaDBGTID, canMove bool) {
	isOracleGTID = (instance.UsingOracleGTID && otherInstance.SupportsOracleGTID)
	isMariaDBGTID = (instance.UsingMariaDBGTID && otherInstance.IsMariaDB())

	return isOracleGTID, isMariaDBGTID, isOracleGTID || isMariaDBGTID
}

// moveInstanceBelowViaGTID will attempt moving given instance below another instance using either Oracle GTID or MariaDB GTID.
func moveInstanceBelowViaGTID(instance, otherInstance *Instance) (*Instance, error) {
	_, _, canMove := canMoveViaGTID(instance, otherInstance)

	instanceKey := &instance.Key
	otherInstanceKey := &otherInstance.Key
	if !canMove {
		return instance, fmt.Errorf("Cannot move via GTID as not both instances use GTID: %+v, %+v", *instanceKey, *otherInstanceKey)
	}

	var err error

	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMoveViaMatch(); !canMove {
		return instance, merr
	}

	if canReplicate, err := instance.CanReplicateFrom(otherInstance); !canReplicate {
		return instance, err
	}
	log.Infof("Will move %+v below %+v via GTID", instanceKey, otherInstanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("move below %+v", *otherInstanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMainTo(instanceKey, &otherInstance.Key, &otherInstance.SelfBinlogCoordinates, false, GTIDHintForce)
	if err != nil {
		goto Cleanup
	}
Cleanup:
	instance, _ = StartSubordinate(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("move-below-gtid", instanceKey, fmt.Sprintf("moved %+v below %+v", *instanceKey, *otherInstanceKey))

	return instance, err
}

// MoveBelowGTID will attempt moving instance indicated by instanceKey below another instance using either Oracle GTID or MariaDB GTID.
func MoveBelowGTID(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	other, err := ReadTopologyInstanceUnbuffered(otherKey)
	if err != nil {
		return instance, err
	}
	return moveInstanceBelowViaGTID(instance, other)
}

// moveSubordinatesViaGTID moves a list of subordinates under another instance via GTID, returning those subordinates
// that could not be moved (do not use GTID)
func moveSubordinatesViaGTID(subordinates [](*Instance), other *Instance) (movedSubordinates [](*Instance), unmovedSubordinates [](*Instance), err error, errs []error) {
	subordinates = RemoveInstance(subordinates, &other.Key)
	if len(subordinates) == 0 {
		// Nothing to do
		return movedSubordinates, unmovedSubordinates, nil, errs
	}

	log.Infof("Will move %+v subordinates below %+v via GTID", len(subordinates), other.Key)

	barrier := make(chan *InstanceKey)
	subordinateMutex := make(chan bool, 1)
	for _, subordinate := range subordinates {
		subordinate := subordinate

		// Parallelize repoints
		go func() {
			defer func() { barrier <- &subordinate.Key }()
			ExecuteOnTopology(func() {
				var subordinateErr error
				if _, _, canMove := canMoveViaGTID(subordinate, other); canMove {
					subordinate, subordinateErr = moveInstanceBelowViaGTID(subordinate, other)
				} else {
					subordinateErr = fmt.Errorf("%+v cannot move below %+v via GTID", subordinate.Key, other.Key)
				}
				func() {
					// Instantaneous mutex.
					subordinateMutex <- true
					defer func() { <-subordinateMutex }()
					if subordinateErr == nil {
						movedSubordinates = append(movedSubordinates, subordinate)
					} else {
						unmovedSubordinates = append(unmovedSubordinates, subordinate)
						errs = append(errs, subordinateErr)
					}
				}()
			})
		}()
	}
	for range subordinates {
		<-barrier
	}
	if len(errs) == len(subordinates) {
		// All returned with error
		return movedSubordinates, unmovedSubordinates, fmt.Errorf("moveSubordinatesViaGTID: Error on all %+v operations", len(errs)), errs
	}
	AuditOperation("move-subordinates-gtid", &other.Key, fmt.Sprintf("moved %d/%d subordinates below %+v via GTID", len(movedSubordinates), len(subordinates), other.Key))

	return movedSubordinates, unmovedSubordinates, err, errs
}

// MoveSubordinatesGTID will (attempt to) move all subordinates of given main below given instance.
func MoveSubordinatesGTID(mainKey *InstanceKey, belowKey *InstanceKey, pattern string) (movedSubordinates [](*Instance), unmovedSubordinates [](*Instance), err error, errs []error) {
	belowInstance, err := ReadTopologyInstanceUnbuffered(belowKey)
	if err != nil {
		// Can't access "below" ==> can't move subordinates beneath it
		return movedSubordinates, unmovedSubordinates, err, errs
	}

	// subordinates involved
	subordinates, err := ReadSubordinateInstancesIncludingBinlogServerSubSubordinates(mainKey)
	if err != nil {
		return movedSubordinates, unmovedSubordinates, err, errs
	}
	subordinates = filterInstancesByPattern(subordinates, pattern)
	movedSubordinates, unmovedSubordinates, err, errs = moveSubordinatesViaGTID(subordinates, belowInstance)
	if err != nil {
		log.Errore(err)
	}

	if len(unmovedSubordinates) > 0 {
		err = fmt.Errorf("MoveSubordinatesGTID: only moved %d out of %d subordinates of %+v; error is: %+v", len(movedSubordinates), len(subordinates), *mainKey, err)
	}

	return movedSubordinates, unmovedSubordinates, err, errs
}

// Repoint connects a subordinate to a main using its exact same executing coordinates.
// The given mainKey can be null, in which case the existing main is used.
// Two use cases:
// - mainKey is nil: use case is corrupted relay logs on subordinate
// - mainKey is not nil: using Binlog servers (coordinates remain the same)
func Repoint(instanceKey *InstanceKey, mainKey *InstanceKey, gtidHint OperationGTIDHint) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsSubordinate() {
		return instance, fmt.Errorf("instance is not a subordinate: %+v", *instanceKey)
	}

	if mainKey == nil {
		mainKey = &instance.MainKey
	}
	// With repoint we *prefer* the main to be alive, but we don't strictly require it.
	// The use case for the main being alive is with hostname-resolve or hostname-unresolve: asking the subordinate
	// to reconnect to its same main while changing the MASTER_HOST in CHANGE MASTER TO due to DNS changes etc.
	main, err := ReadTopologyInstanceUnbuffered(mainKey)
	mainIsAccessible := (err == nil)
	if !mainIsAccessible {
		main, _, err = ReadInstance(mainKey)
		if err != nil {
			return instance, err
		}
	}
	if canReplicate, err := instance.CanReplicateFrom(main); !canReplicate {
		return instance, err
	}

	// if a binlog server check it is sufficiently up to date
	if main.IsBinlogServer() {
		// "Repoint" operation trusts the user. But only so much. Repoiting to a binlog server which is not yet there is strictly wrong.
		if !instance.ExecBinlogCoordinates.SmallerThanOrEquals(&main.SelfBinlogCoordinates) {
			return instance, fmt.Errorf("repoint: binlog server %+v is not sufficiently up to date to repoint %+v below it", *mainKey, *instanceKey)
		}
	}

	log.Infof("Will repoint %+v to main %+v", *instanceKey, *mainKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "repoint"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

	// See above, we are relaxed about the main being accessible/inaccessible.
	// If accessible, we wish to do hostname-unresolve. If inaccessible, we can skip the test and not fail the
	// ChangeMainTo operation. This is why we pass "!mainIsAccessible" below.
	if instance.ExecBinlogCoordinates.IsEmpty() {
		instance.ExecBinlogCoordinates.LogFile = "orchestrator-unknown-log-file"
	}
	instance, err = ChangeMainTo(instanceKey, mainKey, &instance.ExecBinlogCoordinates, !mainIsAccessible, gtidHint)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("repoint", instanceKey, fmt.Sprintf("subordinate %+v repointed to main: %+v", *instanceKey, *mainKey))

	return instance, err

}

// RepointTo repoints list of subordinates onto another main.
// Binlog Server is the major use case
func RepointTo(subordinates [](*Instance), belowKey *InstanceKey) ([](*Instance), error, []error) {
	res := [](*Instance){}
	errs := []error{}

	subordinates = RemoveInstance(subordinates, belowKey)
	if len(subordinates) == 0 {
		// Nothing to do
		return res, nil, errs
	}
	if belowKey == nil {
		return res, log.Errorf("RepointTo received nil belowKey"), errs
	}

	log.Infof("Will repoint %+v subordinates below %+v", len(subordinates), *belowKey)
	barrier := make(chan *InstanceKey)
	subordinateMutex := make(chan bool, 1)
	for _, subordinate := range subordinates {
		subordinate := subordinate

		// Parallelize repoints
		go func() {
			defer func() { barrier <- &subordinate.Key }()
			ExecuteOnTopology(func() {
				subordinate, subordinateErr := Repoint(&subordinate.Key, belowKey, GTIDHintNeutral)

				func() {
					// Instantaneous mutex.
					subordinateMutex <- true
					defer func() { <-subordinateMutex }()
					if subordinateErr == nil {
						res = append(res, subordinate)
					} else {
						errs = append(errs, subordinateErr)
					}
				}()
			})
		}()
	}
	for range subordinates {
		<-barrier
	}

	if len(errs) == len(subordinates) {
		// All returned with error
		return res, log.Error("Error on all operations"), errs
	}
	AuditOperation("repoint-to", belowKey, fmt.Sprintf("repointed %d/%d subordinates to %+v", len(res), len(subordinates), *belowKey))

	return res, nil, errs
}

// RepointSubordinatesTo repoints subordinates of a given instance (possibly filtered) onto another main.
// Binlog Server is the major use case
func RepointSubordinatesTo(instanceKey *InstanceKey, pattern string, belowKey *InstanceKey) ([](*Instance), error, []error) {
	res := [](*Instance){}
	errs := []error{}

	subordinates, err := ReadSubordinateInstances(instanceKey)
	if err != nil {
		return res, err, errs
	}
	subordinates = RemoveInstance(subordinates, belowKey)
	subordinates = filterInstancesByPattern(subordinates, pattern)
	if len(subordinates) == 0 {
		// Nothing to do
		return res, nil, errs
	}
	if belowKey == nil {
		// Default to existing main. All subordinates are of the same main, hence just pick one.
		belowKey = &subordinates[0].MainKey
	}
	log.Infof("Will repoint subordinates of %+v to %+v", *instanceKey, *belowKey)
	return RepointTo(subordinates, belowKey)
}

// RepointSubordinates repoints all subordinates of a given instance onto its existing main.
func RepointSubordinates(instanceKey *InstanceKey, pattern string) ([](*Instance), error, []error) {
	return RepointSubordinatesTo(instanceKey, pattern, nil)
}

// MakeCoMain will attempt to make an instance co-main with its main, by making its main a subordinate of its own.
// This only works out if the main is not replicating; the main does not have a known main (it may have an unknown main).
func MakeCoMain(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	if canMove, merr := instance.CanMove(); !canMove {
		return instance, merr
	}
	main, err := GetInstanceMain(instance)
	if err != nil {
		return instance, err
	}
	log.Debugf("Will check whether %+v's main (%+v) can become its co-main", instance.Key, main.Key)
	if canMove, merr := main.CanMoveAsCoMain(); !canMove {
		return instance, merr
	}
	if instanceKey.Equals(&main.MainKey) {
		return instance, fmt.Errorf("instance %+v is already co main of %+v", instance.Key, main.Key)
	}
	if !instance.ReadOnly {
		return instance, fmt.Errorf("instance %+v is not read-only; first make it read-only before making it co-main", instance.Key)
	}
	if main.IsCoMain {
		// We allow breaking of an existing co-main replication. Here's the breakdown:
		// Ideally, this would not eb allowed, and we would first require the user to RESET SLAVE on 'main'
		// prior to making it participate as co-main with our 'instance'.
		// However there's the problem that upon RESET SLAVE we lose the replication's user/password info.
		// Thus, we come up with the following rule:
		// If S replicates from M1, and M1<->M2 are co mains, we allow S to become co-main of M1 (S<->M1) if:
		// - M1 is writeable
		// - M2 is read-only or is unreachable/invalid
		// - S  is read-only
		// And so we will be replacing one read-only co-main with another.
		otherCoMain, found, _ := ReadInstance(&main.MainKey)
		if found && otherCoMain.IsLastCheckValid && !otherCoMain.ReadOnly {
			return instance, fmt.Errorf("main %+v is already co-main with %+v, and %+v is alive, and not read-only; cowardly refusing to demote it. Please set it as read-only beforehand", main.Key, otherCoMain.Key, otherCoMain.Key)
		}
		// OK, good to go.
	} else if _, found, _ := ReadInstance(&main.MainKey); found {
		return instance, fmt.Errorf("%+v is not a real main; it replicates from: %+v", main.Key, main.MainKey)
	}
	if canReplicate, err := main.CanReplicateFrom(instance); !canReplicate {
		return instance, err
	}
	log.Infof("Will make %+v co-main of %+v", instanceKey, main.Key)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("make co-main of %+v", main.Key)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := BeginMaintenance(&main.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v turns into co-main of this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", main.Key)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	// the coMain used to be merely a subordinate. Just point main into *some* position
	// within coMain...
	if main.IsSubordinate() {
		// this is the case of a co-main. For mains, the StopSubordinate operation throws an error, and
		// there's really no point in doing it.
		main, err = StopSubordinate(&main.Key)
		if err != nil {
			goto Cleanup
		}
	}
	if instance.ReplicationCredentialsAvailable && !main.HasReplicationCredentials {
		// Yay! We can get credentials from the subordinate!
		replicationUser, replicationPassword, err := ReadReplicationCredentials(&instance.Key)
		if err != nil {
			goto Cleanup
		}
		log.Debugf("Got credentials from a replica. will now apply")
		_, err = ChangeMainCredentials(&main.Key, replicationUser, replicationPassword)
		if err != nil {
			goto Cleanup
		}
	}
	main, err = ChangeMainTo(&main.Key, instanceKey, &instance.SelfBinlogCoordinates, false, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	main, _ = StartSubordinate(&main.Key)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("make-co-main", instanceKey, fmt.Sprintf("%+v made co-main of %+v", *instanceKey, main.Key))

	return instance, err
}

// ResetSubordinateOperation will reset a subordinate
func ResetSubordinateOperation(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}

	log.Infof("Will reset subordinate on %+v", instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reset subordinate"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if instance.IsSubordinate() {
		instance, err = StopSubordinate(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = ResetSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)

	if err != nil {
		return instance, log.Errore(err)
	}

	// and we're done (pending deferred functions)
	AuditOperation("reset-subordinate", instanceKey, fmt.Sprintf("%+v replication reset", *instanceKey))

	return instance, err
}

// DetachSubordinateOperation will detach a subordinate from its main by forcibly corrupting its replication coordinates
func DetachSubordinateOperation(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}

	log.Infof("Will detach %+v", instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "detach subordinate"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if instance.IsSubordinate() {
		instance, err = StopSubordinate(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = DetachSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)

	if err != nil {
		return instance, log.Errore(err)
	}

	// and we're done (pending deferred functions)
	AuditOperation("detach-subordinate", instanceKey, fmt.Sprintf("%+v replication detached", *instanceKey))

	return instance, err
}

// ReattachSubordinateOperation will detach a subordinate from its main by forcibly corrupting its replication coordinates
func ReattachSubordinateOperation(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}

	log.Infof("Will reattach %+v", instanceKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "detach subordinate"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if instance.IsSubordinate() {
		instance, err = StopSubordinate(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = ReattachSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)

	if err != nil {
		return instance, log.Errore(err)
	}

	// and we're done (pending deferred functions)
	AuditOperation("reattach-subordinate", instanceKey, fmt.Sprintf("%+v replication reattached", *instanceKey))

	return instance, err
}

// DetachSubordinateMainHost detaches a subordinate from its main by corrupting the Main_Host (in such way that is reversible)
func DetachSubordinateMainHost(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsSubordinate() {
		return instance, fmt.Errorf("instance is not a subordinate: %+v", *instanceKey)
	}
	if instance.MainKey.IsDetached() {
		return instance, fmt.Errorf("instance already detached: %+v", *instanceKey)
	}
	detachedMainKey := instance.MainKey.DetachedKey()

	log.Infof("Will detach main host on %+v. Detached key is %+v", *instanceKey, *detachedMainKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "detach-subordinate-main-host"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMainTo(instanceKey, detachedMainKey, &instance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("repoint", instanceKey, fmt.Sprintf("subordinate %+v detached from main into %+v", *instanceKey, *detachedMainKey))

	return instance, err
}

// ReattachSubordinateMainHost reattaches a subordinate back onto its main by undoing a DetachSubordinateMainHost operation
func ReattachSubordinateMainHost(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.IsSubordinate() {
		return instance, fmt.Errorf("instance is not a subordinate: %+v", *instanceKey)
	}
	if !instance.MainKey.IsDetached() {
		return instance, fmt.Errorf("instance does not seem to be detached: %+v", *instanceKey)
	}

	reattachedMainKey := instance.MainKey.ReattachedKey()

	log.Infof("Will reattach main host on %+v. Reattached key is %+v", *instanceKey, *reattachedMainKey)

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reattach-subordinate-main-host"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMainTo(instanceKey, reattachedMainKey, &instance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// Just in case this instance used to be a main:
	ReplaceAliasClusterName(instanceKey.StringCode(), reattachedMainKey.StringCode())

Cleanup:
	instance, _ = StartSubordinate(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("repoint", instanceKey, fmt.Sprintf("subordinate %+v reattached to main %+v", *instanceKey, *reattachedMainKey))

	return instance, err
}

// EnableGTID will attempt to enable GTID-mode (either Oracle or MariaDB)
func EnableGTID(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	if instance.UsingGTID() {
		return instance, fmt.Errorf("%+v already uses GTID", *instanceKey)
	}

	log.Infof("Will attempt to enable GTID on %+v", *instanceKey)

	instance, err = Repoint(instanceKey, nil, GTIDHintForce)
	if err != nil {
		return instance, err
	}
	if !instance.UsingGTID() {
		return instance, fmt.Errorf("Cannot enable GTID on %+v", *instanceKey)
	}

	AuditOperation("enable-gtid", instanceKey, fmt.Sprintf("enabled GTID on %+v", *instanceKey))

	return instance, err
}

// DisableGTID will attempt to disable GTID-mode (either Oracle or MariaDB) and revert to binlog file:pos replication
func DisableGTID(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.UsingGTID() {
		return instance, fmt.Errorf("%+v is not using GTID", *instanceKey)
	}

	log.Infof("Will attempt to disable GTID on %+v", *instanceKey)

	instance, err = Repoint(instanceKey, nil, GTIDHintDeny)
	if err != nil {
		return instance, err
	}
	if instance.UsingGTID() {
		return instance, fmt.Errorf("Cannot disable GTID on %+v", *instanceKey)
	}

	AuditOperation("disable-gtid", instanceKey, fmt.Sprintf("disabled GTID on %+v", *instanceKey))

	return instance, err
}

// ResetMainGTIDOperation will issue a safe RESET MASTER on a subordinate that replicates via GTID:
// It will make sure the gtid_purged set matches the executed set value as read just before the RESET.
// this will enable new subordinates to be attached to given instance without complaints about missing/purged entries.
// This function requires that the instance does not have subordinates.
func ResetMainGTIDOperation(instanceKey *InstanceKey, removeSelfUUID bool, uuidToRemove string) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	if !instance.SupportsOracleGTID {
		return instance, log.Errorf("reset-main-gtid requested for %+v but it is not using oracle-gtid", *instanceKey)
	}
	if len(instance.SubordinateHosts) > 0 {
		return instance, log.Errorf("reset-main-gtid will not operate on %+v because it has %+v subordinates. Expecting no subordinates", *instanceKey, len(instance.SubordinateHosts))
	}

	log.Infof("Will reset main on %+v", instanceKey)

	var oracleGtidSet *OracleGtidSet
	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), "reset-main-gtid"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	if instance.IsSubordinate() {
		instance, err = StopSubordinate(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	oracleGtidSet, err = ParseGtidSet(instance.ExecutedGtidSet)
	if err != nil {
		goto Cleanup
	}
	if removeSelfUUID {
		uuidToRemove = instance.ServerUUID
	}
	if uuidToRemove != "" {
		removed := oracleGtidSet.RemoveUUID(uuidToRemove)
		if removed {
			log.Debugf("Will remove UUID %s", uuidToRemove)
		} else {
			log.Debugf("UUID %s not found", uuidToRemove)
		}
	}

	instance, err = ResetMain(instanceKey)
	if err != nil {
		goto Cleanup
	}
	err = setGTIDPurged(instance, oracleGtidSet.String())
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)

	if err != nil {
		return instance, log.Errore(err)
	}

	// and we're done (pending deferred functions)
	AuditOperation("reset-main-gtid", instanceKey, fmt.Sprintf("%+v main reset", *instanceKey))

	return instance, err
}

// FindLastPseudoGTIDEntry will search an instance's binary logs or relay logs for the last pseudo-GTID entry,
// and return found coordinates as well as entry text
func FindLastPseudoGTIDEntry(instance *Instance, recordedInstanceRelayLogCoordinates BinlogCoordinates, maxBinlogCoordinates *BinlogCoordinates, exhaustiveSearch bool, expectedBinlogFormat *string) (instancePseudoGtidCoordinates *BinlogCoordinates, instancePseudoGtidText string, err error) {

	if config.Config.PseudoGTIDPattern == "" {
		return instancePseudoGtidCoordinates, instancePseudoGtidText, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}

	minBinlogCoordinates, minRelaylogCoordinates, err := GetHeuristiclyRecentCoordinatesForInstance(&instance.Key)
	if instance.LogBinEnabled && instance.LogSubordinateUpdatesEnabled && (expectedBinlogFormat == nil || instance.Binlog_format == *expectedBinlogFormat) {
		// Well no need to search this instance's binary logs if it doesn't have any...
		// With regard log-subordinate-updates, some edge cases are possible, like having this instance's log-subordinate-updates
		// enabled/disabled (of course having restarted it)
		// The approach is not to take chances. If log-subordinate-updates is disabled, fail and go for relay-logs.
		// If log-subordinate-updates was just enabled then possibly no pseudo-gtid is found, and so again we will go
		// for relay logs.
		// Also, if main has STATEMENT binlog format, and the subordinate has ROW binlog format, then comparing binlog entries would urely fail if based on the subordinate's binary logs.
		// Instead, we revert to the relay logs.
		instancePseudoGtidCoordinates, instancePseudoGtidText, err = getLastPseudoGTIDEntryInInstance(instance, minBinlogCoordinates, maxBinlogCoordinates, exhaustiveSearch)
	}
	if err != nil || instancePseudoGtidCoordinates == nil {
		// Unable to find pseudo GTID in binary logs.
		// Then MAYBE we are lucky enough (chances are we are, if this subordinate did not crash) that we can
		// extract the Pseudo GTID entry from the last (current) relay log file.
		instancePseudoGtidCoordinates, instancePseudoGtidText, err = getLastPseudoGTIDEntryInRelayLogs(instance, minRelaylogCoordinates, recordedInstanceRelayLogCoordinates, exhaustiveSearch)
	}
	return instancePseudoGtidCoordinates, instancePseudoGtidText, err
}

// CorrelateBinlogCoordinates find out, if possible, the binlog coordinates of given otherInstance that correlate
// with given coordinates of given instance.
func CorrelateBinlogCoordinates(instance *Instance, binlogCoordinates *BinlogCoordinates, otherInstance *Instance) (*BinlogCoordinates, int, error) {
	// We record the relay log coordinates just after the instance stopped since the coordinates can change upon
	// a FLUSH LOGS/FLUSH RELAY LOGS (or a START SLAVE, though that's an altogether different problem) etc.
	// We want to be on the safe side; we don't utterly trust that we are the only ones playing with the instance.
	recordedInstanceRelayLogCoordinates := instance.RelaylogCoordinates
	instancePseudoGtidCoordinates, instancePseudoGtidText, err := FindLastPseudoGTIDEntry(instance, recordedInstanceRelayLogCoordinates, binlogCoordinates, true, &otherInstance.Binlog_format)

	if err != nil {
		return nil, 0, err
	}
	entriesMonotonic := (config.Config.PseudoGTIDMonotonicHint != "") && strings.Contains(instancePseudoGtidText, config.Config.PseudoGTIDMonotonicHint)
	minBinlogCoordinates, _, err := GetHeuristiclyRecentCoordinatesForInstance(&otherInstance.Key)
	otherInstancePseudoGtidCoordinates, err := SearchEntryInInstanceBinlogs(otherInstance, instancePseudoGtidText, entriesMonotonic, minBinlogCoordinates)
	if err != nil {
		return nil, 0, err
	}

	// We've found a match: the latest Pseudo GTID position within instance and its identical twin in otherInstance
	// We now iterate the events in both, up to the completion of events in instance (recall that we looked for
	// the last entry in instance, hence, assuming pseudo GTID entries are frequent, the amount of entries to read
	// from instance is not long)
	// The result of the iteration will be either:
	// - bad conclusion that instance is actually more advanced than otherInstance (we find more entries in instance
	//   following the pseudo gtid than we can match in otherInstance), hence we cannot ask instance to replicate
	//   from otherInstance
	// - good result: both instances are exactly in same shape (have replicated the exact same number of events since
	//   the last pseudo gtid). Since they are identical, it is easy to point instance into otherInstance.
	// - good result: the first position within otherInstance where instance has not replicated yet. It is easy to point
	//   instance into otherInstance.
	nextBinlogCoordinatesToMatch, countMatchedEvents, err := GetNextBinlogCoordinatesToMatch(instance, *instancePseudoGtidCoordinates,
		recordedInstanceRelayLogCoordinates, binlogCoordinates, otherInstance, *otherInstancePseudoGtidCoordinates)
	if err != nil {
		return nil, 0, err
	}
	if countMatchedEvents == 0 {
		err = fmt.Errorf("Unexpected: 0 events processed while iterating logs. Something went wrong; aborting. nextBinlogCoordinatesToMatch: %+v", nextBinlogCoordinatesToMatch)
		return nil, 0, err
	}
	return nextBinlogCoordinatesToMatch, countMatchedEvents, nil
}

// MatchBelow will attempt moving instance indicated by instanceKey below its the one indicated by otherKey.
// The refactoring is based on matching binlog entries, not on "classic" positions comparisons.
// The "other instance" could be the sibling of the moving instance any of its ancestors. It may actually be
// a cousin of some sort (though unlikely). The only important thing is that the "other instance" is more
// advanced in replication than given instance.
func MatchBelow(instanceKey, otherKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, nil, err
	}
	if config.Config.PseudoGTIDPattern == "" {
		return instance, nil, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}
	if instanceKey.Equals(otherKey) {
		return instance, nil, fmt.Errorf("MatchBelow: attempt to match an instance below itself %+v", *instanceKey)
	}
	otherInstance, err := ReadTopologyInstanceUnbuffered(otherKey)
	if err != nil {
		return instance, nil, err
	}

	rinstance, _, _ := ReadInstance(&instance.Key)
	if canMove, merr := rinstance.CanMoveViaMatch(); !canMove {
		return instance, nil, merr
	}

	if canReplicate, err := instance.CanReplicateFrom(otherInstance); !canReplicate {
		return instance, nil, err
	}
	var nextBinlogCoordinatesToMatch *BinlogCoordinates
	var countMatchedEvents int

	if otherInstance.IsBinlogServer() {
		// A Binlog Server does not do all the SHOW BINLOG EVENTS stuff
		err = fmt.Errorf("Cannot use PseudoGTID with Binlog Server %+v", otherInstance.Key)
		goto Cleanup
	}

	log.Infof("Will match %+v below %+v", *instanceKey, *otherKey)

	if requireInstanceMaintenance {
		if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("match below %+v", *otherKey)); merr != nil {
			err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
			goto Cleanup
		} else {
			defer EndMaintenance(maintenanceToken)
		}
	}

	log.Debugf("Stopping subordinate on %+v", *instanceKey)
	instance, err = StopSubordinate(instanceKey)
	if err != nil {
		goto Cleanup
	}

	nextBinlogCoordinatesToMatch, countMatchedEvents, err = CorrelateBinlogCoordinates(instance, nil, otherInstance)

	if countMatchedEvents == 0 {
		err = fmt.Errorf("Unexpected: 0 events processed while iterating logs. Something went wrong; aborting. nextBinlogCoordinatesToMatch: %+v", nextBinlogCoordinatesToMatch)
		goto Cleanup
	}
	log.Debugf("%+v will match below %+v at %+v; validated events: %d", *instanceKey, *otherKey, *nextBinlogCoordinatesToMatch, countMatchedEvents)

	// Drum roll......
	instance, err = ChangeMainTo(instanceKey, otherKey, nextBinlogCoordinatesToMatch, false, GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartSubordinate(instanceKey)
	if err != nil {
		return instance, nextBinlogCoordinatesToMatch, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("match-below", instanceKey, fmt.Sprintf("matched %+v below %+v", *instanceKey, *otherKey))

	return instance, nextBinlogCoordinatesToMatch, err
}

// RematchSubordinate will re-match a subordinate to its main, using pseudo-gtid
func RematchSubordinate(instanceKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, nil, err
	}
	mainInstance, found, err := ReadInstance(&instance.MainKey)
	if err != nil || !found {
		return instance, nil, err
	}
	return MatchBelow(instanceKey, &mainInstance.Key, requireInstanceMaintenance)
}

// MakeMain will take an instance, make all its siblings its subordinates (via pseudo-GTID) and make it main
// (stop its replicaiton, make writeable).
func MakeMain(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	mainInstance, err := ReadTopologyInstanceUnbuffered(&instance.MainKey)
	if err == nil {
		// If the read succeeded, check the main status.
		if mainInstance.IsSubordinate() {
			return instance, fmt.Errorf("MakeMain: instance's main %+v seems to be replicating", mainInstance.Key)
		}
		if mainInstance.IsLastCheckValid {
			return instance, fmt.Errorf("MakeMain: instance's main %+v seems to be accessible", mainInstance.Key)
		}
	}
	// Continue anyway if the read failed, because that means the main is
	// inaccessible... So it's OK to do the promotion.
	if !instance.SQLThreadUpToDate() {
		return instance, fmt.Errorf("MakeMain: instance's SQL thread must be up-to-date with I/O thread for %+v", *instanceKey)
	}
	siblings, err := ReadSubordinateInstances(&mainInstance.Key)
	if err != nil {
		return instance, err
	}
	for _, sibling := range siblings {
		if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
			return instance, fmt.Errorf("MakeMain: instance %+v has more advanced sibling: %+v", *instanceKey, sibling.Key)
		}
	}

	if maintenanceToken, merr := BeginMaintenance(instanceKey, GetMaintenanceOwner(), fmt.Sprintf("siblings match below this: %+v", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v", *instanceKey)
		goto Cleanup
	} else {
		defer EndMaintenance(maintenanceToken)
	}

	_, _, err, _ = MultiMatchBelow(siblings, instanceKey, false, nil)
	if err != nil {
		goto Cleanup
	}

	SetReadOnly(instanceKey, false)

Cleanup:
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("make-main", instanceKey, fmt.Sprintf("made main of %+v", *instanceKey))

	return instance, err
}

// EnsubordinateSiblings is a convenience method for turning sublings of a subordinate to be its subordinates.
// This uses normal connected replication (does not utilize Pseudo-GTID)
func EnsubordinateSiblings(instanceKey *InstanceKey) (*Instance, int, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, 0, err
	}
	mainInstance, found, err := ReadInstance(&instance.MainKey)
	if err != nil || !found {
		return instance, 0, err
	}
	siblings, err := ReadSubordinateInstances(&mainInstance.Key)
	if err != nil {
		return instance, 0, err
	}
	ensubordinatedSiblings := 0
	for _, sibling := range siblings {
		if _, err := MoveBelow(&sibling.Key, &instance.Key); err == nil {
			ensubordinatedSiblings++
		}
	}

	return instance, ensubordinatedSiblings, err
}

// EnsubordinateMain will move an instance up the chain and cause its main to become its subordinate.
// It's almost a role change, just that other subordinates of either 'instance' or its main are currently unaffected
// (they continue replicate without change)
// Note that the main must itself be a subordinate; however the grandparent does not necessarily have to be reachable
// and can in fact be dead.
func EnsubordinateMain(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	mainInstance, found, err := ReadInstance(&instance.MainKey)
	if err != nil || !found {
		return instance, err
	}
	log.Debugf("EnsubordinateMain: will attempt making %+v ensubordinate its main %+v, now resolved as %+v", *instanceKey, instance.MainKey, mainInstance.Key)

	if canReplicate, err := mainInstance.CanReplicateFrom(instance); canReplicate == false {
		return instance, err
	}
	// We begin
	mainInstance, err = StopSubordinate(&mainInstance.Key)
	if err != nil {
		goto Cleanup
	}
	instance, err = StopSubordinate(&instance.Key)
	if err != nil {
		goto Cleanup
	}

	instance, err = StartSubordinateUntilMainCoordinates(&instance.Key, &mainInstance.SelfBinlogCoordinates)
	if err != nil {
		goto Cleanup
	}

	// instance and mainInstance are equal
	// We skip name unresolve. It is OK if the main's main is dead, unreachable, does not resolve properly.
	// We just copy+paste info from the main.
	// In particular, this is commonly calledin DeadMain recovery
	instance, err = ChangeMainTo(&instance.Key, &mainInstance.MainKey, &mainInstance.ExecBinlogCoordinates, true, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// instance is now sibling of main
	mainInstance, err = ChangeMainTo(&mainInstance.Key, &instance.Key, &instance.SelfBinlogCoordinates, false, GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// swap is done!

Cleanup:
	instance, _ = StartSubordinate(&instance.Key)
	mainInstance, _ = StartSubordinate(&mainInstance.Key)
	if err != nil {
		return instance, err
	}
	AuditOperation("ensubordinate-main", instanceKey, fmt.Sprintf("ensubordinated main: %+v", mainInstance.Key))

	return instance, err
}

// MakeLocalMain promotes a subordinate above its main, making it subordinate of its grandparent, while also enslaving its siblings.
// This serves as a convenience method to recover replication when a local main fails; the instance promoted is one of its subordinates,
// which is most advanced among its siblings.
// This method utilizes Pseudo GTID
func MakeLocalMain(instanceKey *InstanceKey) (*Instance, error) {
	instance, err := ReadTopologyInstanceUnbuffered(instanceKey)
	if err != nil {
		return instance, err
	}
	mainInstance, found, err := ReadInstance(&instance.MainKey)
	if err != nil || !found {
		return instance, err
	}
	grandparentInstance, err := ReadTopologyInstanceUnbuffered(&mainInstance.MainKey)
	if err != nil {
		return instance, err
	}
	siblings, err := ReadSubordinateInstances(&mainInstance.Key)
	if err != nil {
		return instance, err
	}
	for _, sibling := range siblings {
		if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
			return instance, fmt.Errorf("MakeMain: instance %+v has more advanced sibling: %+v", *instanceKey, sibling.Key)
		}
	}

	instance, err = StopSubordinateNicely(instanceKey, 0)
	if err != nil {
		goto Cleanup
	}

	_, _, err = MatchBelow(instanceKey, &grandparentInstance.Key, true)
	if err != nil {
		goto Cleanup
	}

	_, _, err, _ = MultiMatchBelow(siblings, instanceKey, false, nil)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	AuditOperation("make-local-main", instanceKey, fmt.Sprintf("made main of %+v", *instanceKey))

	return instance, err
}

// sortInstances shuffles given list of instances according to some logic
func sortInstances(instances [](*Instance)) {
	sort.Sort(sort.Reverse(InstancesByExecBinlogCoordinates(instances)))
}

// getSubordinatesForSorting returns a list of subordinates of a given main potentially for candidate choosing
func getSubordinatesForSorting(mainKey *InstanceKey, includeBinlogServerSubSubordinates bool) (subordinates [](*Instance), err error) {
	if includeBinlogServerSubSubordinates {
		subordinates, err = ReadSubordinateInstancesIncludingBinlogServerSubSubordinates(mainKey)
	} else {
		subordinates, err = ReadSubordinateInstances(mainKey)
	}
	return subordinates, err
}

// sortedSubordinates returns the list of subordinates of some main, sorted by exec coordinates
// (most up-to-date subordinate first).
// This function assumes given `subordinates` argument is indeed a list of instances all replicating
// from the same main (the result of `getSubordinatesForSorting()` is appropriate)
func sortedSubordinates(subordinates [](*Instance), shouldStopSubordinates bool) [](*Instance) {
	if len(subordinates) == 0 {
		return subordinates
	}
	if shouldStopSubordinates {
		log.Debugf("sortedSubordinates: stopping %d subordinates nicely", len(subordinates))
		subordinates = StopSubordinatesNicely(subordinates, time.Duration(config.Config.InstanceBulkOperationsWaitTimeoutSeconds)*time.Second)
	}
	subordinates = RemoveNilInstances(subordinates)

	sortInstances(subordinates)
	for _, subordinate := range subordinates {
		log.Debugf("- sorted subordinate: %+v %+v", subordinate.Key, subordinate.ExecBinlogCoordinates)
	}

	return subordinates
}

// MultiMatchBelow will efficiently match multiple subordinates below a given instance.
// It is assumed that all given subordinates are siblings
func MultiMatchBelow(subordinates [](*Instance), belowKey *InstanceKey, subordinatesAlreadyStopped bool, postponedFunctionsContainer *PostponedFunctionsContainer) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}
	subordinateMutex := make(chan bool, 1)

	if config.Config.PseudoGTIDPattern == "" {
		return res, nil, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID"), errs
	}

	subordinates = RemoveInstance(subordinates, belowKey)
	subordinates = RemoveBinlogServerInstances(subordinates)

	for _, subordinate := range subordinates {
		if maintenanceToken, merr := BeginMaintenance(&subordinate.Key, GetMaintenanceOwner(), fmt.Sprintf("%+v match below %+v as part of MultiMatchBelow", subordinate.Key, *belowKey)); merr != nil {
			errs = append(errs, fmt.Errorf("Cannot begin maintenance on %+v", subordinate.Key))
			subordinates = RemoveInstance(subordinates, &subordinate.Key)
		} else {
			defer EndMaintenance(maintenanceToken)
		}
	}

	belowInstance, err := ReadTopologyInstanceUnbuffered(belowKey)
	if err != nil {
		// Can't access the server below which we need to match ==> can't move subordinates
		return res, belowInstance, err, errs
	}
	if belowInstance.IsBinlogServer() {
		// A Binlog Server does not do all the SHOW BINLOG EVENTS stuff
		err = fmt.Errorf("Cannot use PseudoGTID with Binlog Server %+v", belowInstance.Key)
		return res, belowInstance, err, errs
	}

	// subordinates involved
	if len(subordinates) == 0 {
		return res, belowInstance, nil, errs
	}
	if !subordinatesAlreadyStopped {
		log.Debugf("MultiMatchBelow: stopping %d subordinates nicely", len(subordinates))
		// We want the subordinates to have SQL thread up to date with IO thread.
		// We will wait for them (up to a timeout) to do so.
		subordinates = StopSubordinatesNicely(subordinates, time.Duration(config.Config.InstanceBulkOperationsWaitTimeoutSeconds)*time.Second)
	}
	subordinates = RemoveNilInstances(subordinates)
	sort.Sort(sort.Reverse(InstancesByExecBinlogCoordinates(subordinates)))

	// Optimizations:
	// Subordinates which broke on the same Exec-coordinates can be handled in the exact same way:
	// we only need to figure out one subordinate of each group/bucket of exec-coordinates; then apply the CHANGE MASTER TO
	// on all its fellow members using same coordinates.
	subordinateBuckets := make(map[BinlogCoordinates][](*Instance))
	for _, subordinate := range subordinates {
		subordinate := subordinate
		subordinateBuckets[subordinate.ExecBinlogCoordinates] = append(subordinateBuckets[subordinate.ExecBinlogCoordinates], subordinate)
	}
	log.Debugf("MultiMatchBelow: %d subordinates merged into %d buckets", len(subordinates), len(subordinateBuckets))
	for bucket, bucketSubordinates := range subordinateBuckets {
		log.Debugf("+- bucket: %+v, %d subordinates", bucket, len(bucketSubordinates))
	}
	matchedSubordinates := make(map[InstanceKey]bool)
	bucketsBarrier := make(chan *BinlogCoordinates)
	// Now go over the buckets, and try a single subordinate from each bucket
	// (though if one results with an error, synchronuously-for-that-bucket continue to the next subordinate in bucket)

	for execCoordinates, bucketSubordinates := range subordinateBuckets {
		execCoordinates := execCoordinates
		bucketSubordinates := bucketSubordinates
		var bucketMatchedCoordinates *BinlogCoordinates
		// Buckets concurrent
		go func() {
			// find coordinates for a single bucket based on a subordinate in said bucket
			defer func() { bucketsBarrier <- &execCoordinates }()
			func() {
				for _, subordinate := range bucketSubordinates {
					subordinate := subordinate
					var subordinateErr error
					var matchedCoordinates *BinlogCoordinates
					log.Debugf("MultiMatchBelow: attempting subordinate %+v in bucket %+v", subordinate.Key, execCoordinates)
					matchFunc := func() error {
						ExecuteOnTopology(func() {
							_, matchedCoordinates, subordinateErr = MatchBelow(&subordinate.Key, &belowInstance.Key, false)
						})
						return nil
					}
					if postponedFunctionsContainer != nil &&
						config.Config.PostponeSubordinateRecoveryOnLagMinutes > 0 &&
						subordinate.SQLDelay > config.Config.PostponeSubordinateRecoveryOnLagMinutes*60 &&
						len(bucketSubordinates) == 1 {
						// This subordinate is the only one in the bucket, AND it's lagging very much, AND
						// we're configured to postpone operation on this subordinate so as not to delay everyone else.
						(*postponedFunctionsContainer).AddPostponedFunction(matchFunc)
						return
						// We bail out and trust our invoker to later call upon this postponed function
					}
					matchFunc()
					log.Debugf("MultiMatchBelow: match result: %+v, %+v", matchedCoordinates, subordinateErr)

					if subordinateErr == nil {
						// Success! We matched a subordinate of this bucket
						func() {
							// Instantaneous mutex.
							subordinateMutex <- true
							defer func() { <-subordinateMutex }()
							bucketMatchedCoordinates = matchedCoordinates
							matchedSubordinates[subordinate.Key] = true
						}()
						log.Debugf("MultiMatchBelow: matched subordinate %+v in bucket %+v", subordinate.Key, execCoordinates)
						return
					}

					// Got here? Error!
					func() {
						// Instantaneous mutex.
						subordinateMutex <- true
						defer func() { <-subordinateMutex }()
						errs = append(errs, subordinateErr)
					}()
					log.Errore(subordinateErr)
					// Failure: some unknown problem with bucket subordinate. Let's try the next one (continue loop)
				}
			}()
			if bucketMatchedCoordinates == nil {
				log.Errorf("MultiMatchBelow: Cannot match up %d subordinates since their bucket %+v is failed", len(bucketSubordinates), execCoordinates)
				return
			}
			log.Debugf("MultiMatchBelow: bucket %+v coordinates are: %+v. Proceeding to match all bucket subordinates", execCoordinates, *bucketMatchedCoordinates)
			// At this point our bucket has a known salvaged subordinate.
			// We don't wait for the other buckets -- we immediately work out all the other subordinates in this bucket.
			// (perhaps another bucket is busy matching a 24h delayed-replica; we definitely don't want to hold on that)
			func() {
				barrier := make(chan *InstanceKey)
				// We point all this bucket's subordinates into the same coordinates, concurrently
				// We are already doing concurrent buckets; but for each bucket we also want to do concurrent subordinates,
				// otherwise one large bucket would make for a sequential work...
				for _, subordinate := range bucketSubordinates {
					subordinate := subordinate
					go func() {
						defer func() { barrier <- &subordinate.Key }()

						var err error
						if _, found := matchedSubordinates[subordinate.Key]; found {
							// Already matched this subordinate
							return
						}
						log.Debugf("MultiMatchBelow: Will match up %+v to previously matched main coordinates %+v", subordinate.Key, *bucketMatchedCoordinates)
						subordinateMatchSuccess := false
						ExecuteOnTopology(func() {
							if _, err = ChangeMainTo(&subordinate.Key, &belowInstance.Key, bucketMatchedCoordinates, false, GTIDHintDeny); err == nil {
								StartSubordinate(&subordinate.Key)
								subordinateMatchSuccess = true
							}
						})
						func() {
							// Quickly update lists; mutext is instantenous
							subordinateMutex <- true
							defer func() { <-subordinateMutex }()
							if subordinateMatchSuccess {
								matchedSubordinates[subordinate.Key] = true
							} else {
								errs = append(errs, err)
								log.Errorf("MultiMatchBelow: Cannot match up %+v: error is %+v", subordinate.Key, err)
							}
						}()
					}()
				}
				for range bucketSubordinates {
					<-barrier
				}
			}()
		}()
	}
	for range subordinateBuckets {
		<-bucketsBarrier
	}

	for _, subordinate := range subordinates {
		subordinate := subordinate
		if _, found := matchedSubordinates[subordinate.Key]; found {
			res = append(res, subordinate)
		}
	}
	return res, belowInstance, err, errs
}

// MultiMatchSubordinates will match (via pseudo-gtid) all subordinates of given main below given instance.
func MultiMatchSubordinates(mainKey *InstanceKey, belowKey *InstanceKey, pattern string) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}

	belowInstance, err := ReadTopologyInstanceUnbuffered(belowKey)
	if err != nil {
		// Can't access "below" ==> can't match subordinates beneath it
		return res, nil, err, errs
	}

	mainInstance, found, err := ReadInstance(mainKey)
	if err != nil || !found {
		return res, nil, err, errs
	}

	// See if we have a binlog server case (special handling):
	binlogCase := false
	if mainInstance.IsBinlogServer() && mainInstance.MainKey.Equals(belowKey) {
		// repoint-up
		log.Debugf("MultiMatchSubordinates: pointing subordinates up from binlog server")
		binlogCase = true
	} else if belowInstance.IsBinlogServer() && belowInstance.MainKey.Equals(mainKey) {
		// repoint-down
		log.Debugf("MultiMatchSubordinates: pointing subordinates down to binlog server")
		binlogCase = true
	} else if mainInstance.IsBinlogServer() && belowInstance.IsBinlogServer() && mainInstance.MainKey.Equals(&belowInstance.MainKey) {
		// Both BLS, siblings
		log.Debugf("MultiMatchSubordinates: pointing subordinates to binlong sibling")
		binlogCase = true
	}
	if binlogCase {
		subordinates, err, errors := RepointSubordinatesTo(mainKey, pattern, belowKey)
		// Bail out!
		return subordinates, mainInstance, err, errors
	}

	// Not binlog server

	// subordinates involved
	subordinates, err := ReadSubordinateInstancesIncludingBinlogServerSubSubordinates(mainKey)
	if err != nil {
		return res, belowInstance, err, errs
	}
	subordinates = filterInstancesByPattern(subordinates, pattern)
	matchedSubordinates, belowInstance, err, errs := MultiMatchBelow(subordinates, &belowInstance.Key, false, nil)

	if len(matchedSubordinates) != len(subordinates) {
		err = fmt.Errorf("MultiMatchSubordinates: only matched %d out of %d subordinates of %+v; error is: %+v", len(matchedSubordinates), len(subordinates), *mainKey, err)
	}
	AuditOperation("multi-match-subordinates", mainKey, fmt.Sprintf("matched %d subordinates under %+v", len(matchedSubordinates), *belowKey))

	return matchedSubordinates, belowInstance, err, errs
}

// MatchUp will move a subordinate up the replication chain, so that it becomes sibling of its main, via Pseudo-GTID
func MatchUp(instanceKey *InstanceKey, requireInstanceMaintenance bool) (*Instance, *BinlogCoordinates, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return nil, nil, err
	}
	if !instance.IsSubordinate() {
		return instance, nil, fmt.Errorf("instance is not a subordinate: %+v", instanceKey)
	}
	main, found, err := ReadInstance(&instance.MainKey)
	if err != nil || !found {
		return instance, nil, log.Errorf("Cannot get main for %+v. error=%+v", instance.Key, err)
	}

	if !main.IsSubordinate() {
		return instance, nil, fmt.Errorf("main is not a subordinate itself: %+v", main.Key)
	}

	return MatchBelow(instanceKey, &main.MainKey, requireInstanceMaintenance)
}

// MatchUpSubordinates will move all subordinates of given main up the replication chain,
// so that they become siblings of their main.
// This should be called when the local main dies, and all its subordinates are to be resurrected via Pseudo-GTID
func MatchUpSubordinates(mainKey *InstanceKey, pattern string) ([](*Instance), *Instance, error, []error) {
	res := [](*Instance){}
	errs := []error{}

	mainInstance, found, err := ReadInstance(mainKey)
	if err != nil || !found {
		return res, nil, err, errs
	}

	return MultiMatchSubordinates(mainKey, &mainInstance.MainKey, pattern)
}

func isGenerallyValidAsBinlogSource(subordinate *Instance) bool {
	if !subordinate.IsLastCheckValid {
		// something wrong with this subordinate right now. We shouldn't hope to be able to promote it
		return false
	}
	if !subordinate.LogBinEnabled {
		return false
	}
	if !subordinate.LogSubordinateUpdatesEnabled {
		return false
	}

	return true
}

func isGenerallyValidAsCandidateSubordinate(subordinate *Instance) bool {
	if !isGenerallyValidAsBinlogSource(subordinate) {
		// does not have binary logs
		return false
	}
	if subordinate.IsBinlogServer() {
		// Can't regroup under a binlog server because it does not support pseudo-gtid related queries such as SHOW BINLOG EVENTS
		return false
	}

	return true
}

// isValidAsCandidateMainInBinlogServerTopology let's us know whether a given subordinate is generally
// valid to promote to be main.
func isValidAsCandidateMainInBinlogServerTopology(subordinate *Instance) bool {
	if !subordinate.IsLastCheckValid {
		// something wrong with this subordinate right now. We shouldn't hope to be able to promote it
		return false
	}
	if !subordinate.LogBinEnabled {
		return false
	}
	if subordinate.LogSubordinateUpdatesEnabled {
		// That's right: we *disallow* log-subordinate-updates
		return false
	}
	if subordinate.IsBinlogServer() {
		return false
	}

	return true
}

func isBannedFromBeingCandidateSubordinate(subordinate *Instance) bool {
	if subordinate.PromotionRule == MustNotPromoteRule {
		log.Debugf("instance %+v is banned because of promotion rule", subordinate.Key)
		return true
	}
	for _, filter := range config.Config.PromotionIgnoreHostnameFilters {
		if matched, _ := regexp.MatchString(filter, subordinate.Key.Hostname); matched {
			return true
		}
	}
	return false
}

// getPriorityMajorVersionForCandidate returns the primary (most common) major version found
// among given instances. This will be used for choosing best candidate for promotion.
func getPriorityMajorVersionForCandidate(subordinates [](*Instance)) (priorityMajorVersion string, err error) {
	if len(subordinates) == 0 {
		return "", log.Errorf("empty subordinates list in getPriorityMajorVersionForCandidate")
	}
	majorVersionsCount := make(map[string]int)
	for _, subordinate := range subordinates {
		majorVersionsCount[subordinate.MajorVersionString()] = majorVersionsCount[subordinate.MajorVersionString()] + 1
	}
	if len(majorVersionsCount) == 1 {
		// all same version, simple case
		return subordinates[0].MajorVersionString(), nil
	}

	currentMaxMajorVersionCount := 0
	for majorVersion, count := range majorVersionsCount {
		if count > currentMaxMajorVersionCount {
			currentMaxMajorVersionCount = count
			priorityMajorVersion = majorVersion
		}
	}
	return priorityMajorVersion, nil
}

// getPriorityBinlogFormatForCandidate returns the primary (most common) binlog format found
// among given instances. This will be used for choosing best candidate for promotion.
func getPriorityBinlogFormatForCandidate(subordinates [](*Instance)) (priorityBinlogFormat string, err error) {
	if len(subordinates) == 0 {
		return "", log.Errorf("empty subordinates list in getPriorityBinlogFormatForCandidate")
	}
	binlogFormatsCount := make(map[string]int)
	for _, subordinate := range subordinates {
		binlogFormatsCount[subordinate.Binlog_format] = binlogFormatsCount[subordinate.Binlog_format] + 1
	}
	if len(binlogFormatsCount) == 1 {
		// all same binlog format, simple case
		return subordinates[0].Binlog_format, nil
	}

	currentMaxBinlogFormatCount := 0
	for binlogFormat, count := range binlogFormatsCount {
		if count > currentMaxBinlogFormatCount {
			currentMaxBinlogFormatCount = count
			priorityBinlogFormat = binlogFormat
		}
	}
	return priorityBinlogFormat, nil
}

// chooseCandidateSubordinate
func chooseCandidateSubordinate(subordinates [](*Instance)) (candidateSubordinate *Instance, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates [](*Instance), err error) {
	if len(subordinates) == 0 {
		return candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, fmt.Errorf("No subordinates found given in chooseCandidateSubordinate")
	}
	priorityMajorVersion, _ := getPriorityMajorVersionForCandidate(subordinates)
	priorityBinlogFormat, _ := getPriorityBinlogFormatForCandidate(subordinates)

	for _, subordinate := range subordinates {
		subordinate := subordinate
		if isGenerallyValidAsCandidateSubordinate(subordinate) &&
			!isBannedFromBeingCandidateSubordinate(subordinate) &&
			!IsSmallerMajorVersion(priorityMajorVersion, subordinate.MajorVersionString()) &&
			!IsSmallerBinlogFormat(priorityBinlogFormat, subordinate.Binlog_format) {
			// this is the one
			candidateSubordinate = subordinate
			break
		}
	}
	if candidateSubordinate == nil {
		// Unable to find a candidate that will main others.
		// Instead, pick a (single) subordinate which is not banned.
		for _, subordinate := range subordinates {
			subordinate := subordinate
			if !isBannedFromBeingCandidateSubordinate(subordinate) {
				// this is the one
				candidateSubordinate = subordinate
				break
			}
		}
		if candidateSubordinate != nil {
			subordinates = RemoveInstance(subordinates, &candidateSubordinate.Key)
		}
		return candidateSubordinate, subordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, fmt.Errorf("chooseCandidateSubordinate: no candidate subordinate found")
	}
	subordinates = RemoveInstance(subordinates, &candidateSubordinate.Key)
	for _, subordinate := range subordinates {
		subordinate := subordinate
		if canReplicate, _ := subordinate.CanReplicateFrom(candidateSubordinate); !canReplicate {
			cannotReplicateSubordinates = append(cannotReplicateSubordinates, subordinate)
		} else if subordinate.ExecBinlogCoordinates.SmallerThan(&candidateSubordinate.ExecBinlogCoordinates) {
			laterSubordinates = append(laterSubordinates, subordinate)
		} else if subordinate.ExecBinlogCoordinates.Equals(&candidateSubordinate.ExecBinlogCoordinates) {
			equalSubordinates = append(equalSubordinates, subordinate)
		} else {
			aheadSubordinates = append(aheadSubordinates, subordinate)
		}
	}
	return candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, err
}

// GetCandidateSubordinate chooses the best subordinate to promote given a (possibly dead) main
func GetCandidateSubordinate(mainKey *InstanceKey, forRematchPurposes bool) (*Instance, [](*Instance), [](*Instance), [](*Instance), [](*Instance), error) {
	var candidateSubordinate *Instance
	aheadSubordinates := [](*Instance){}
	equalSubordinates := [](*Instance){}
	laterSubordinates := [](*Instance){}
	cannotReplicateSubordinates := [](*Instance){}

	subordinates, err := getSubordinatesForSorting(mainKey, false)
	if err != nil {
		return candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, err
	}
	subordinates = sortedSubordinates(subordinates, forRematchPurposes)
	if err != nil {
		return candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, err
	}
	if len(subordinates) == 0 {
		return candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, fmt.Errorf("No subordinates found for %+v", *mainKey)
	}
	candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, err = chooseCandidateSubordinate(subordinates)
	if err != nil {
		return candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, err
	}
	log.Debugf("GetCandidateSubordinate: candidate: %+v, ahead: %d, equal: %d, late: %d, break: %d", candidateSubordinate.Key, len(aheadSubordinates), len(equalSubordinates), len(laterSubordinates), len(cannotReplicateSubordinates))
	return candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, nil
}

// GetCandidateSubordinateOfBinlogServerTopology chooses the best subordinate to promote given a (possibly dead) main
func GetCandidateSubordinateOfBinlogServerTopology(mainKey *InstanceKey) (candidateSubordinate *Instance, err error) {
	subordinates, err := getSubordinatesForSorting(mainKey, true)
	if err != nil {
		return candidateSubordinate, err
	}
	subordinates = sortedSubordinates(subordinates, false)
	if len(subordinates) == 0 {
		return candidateSubordinate, fmt.Errorf("No subordinates found for %+v", *mainKey)
	}
	for _, subordinate := range subordinates {
		subordinate := subordinate
		if candidateSubordinate != nil {
			break
		}
		if isValidAsCandidateMainInBinlogServerTopology(subordinate) && !isBannedFromBeingCandidateSubordinate(subordinate) {
			// this is the one
			candidateSubordinate = subordinate
		}
	}
	if candidateSubordinate != nil {
		log.Debugf("GetCandidateSubordinateOfBinlogServerTopology: returning %+v as candidate subordinate for %+v", candidateSubordinate.Key, *mainKey)
	} else {
		log.Debugf("GetCandidateSubordinateOfBinlogServerTopology: no candidate subordinate found for %+v", *mainKey)
	}
	return candidateSubordinate, err
}

// RegroupSubordinatesPseudoGTID will choose a candidate subordinate of a given instance, and ensubordinate its siblings using pseudo-gtid
func RegroupSubordinatesPseudoGTID(mainKey *InstanceKey, returnSubordinateEvenOnFailureToRegroup bool, onCandidateSubordinateChosen func(*Instance), postponedFunctionsContainer *PostponedFunctionsContainer) ([](*Instance), [](*Instance), [](*Instance), [](*Instance), *Instance, error) {
	candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, err := GetCandidateSubordinate(mainKey, true)
	if err != nil {
		if !returnSubordinateEvenOnFailureToRegroup {
			candidateSubordinate = nil
		}
		return aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, candidateSubordinate, err
	}

	if config.Config.PseudoGTIDPattern == "" {
		return aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, candidateSubordinate, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}

	if onCandidateSubordinateChosen != nil {
		onCandidateSubordinateChosen(candidateSubordinate)
	}

	log.Debugf("RegroupSubordinates: working on %d equals subordinates", len(equalSubordinates))
	barrier := make(chan *InstanceKey)
	for _, subordinate := range equalSubordinates {
		subordinate := subordinate
		// This subordinate has the exact same executing coordinates as the candidate subordinate. This subordinate
		// is *extremely* easy to attach below the candidate subordinate!
		go func() {
			defer func() { barrier <- &candidateSubordinate.Key }()
			ExecuteOnTopology(func() {
				ChangeMainTo(&subordinate.Key, &candidateSubordinate.Key, &candidateSubordinate.SelfBinlogCoordinates, false, GTIDHintDeny)
			})
		}()
	}
	for range equalSubordinates {
		<-barrier
	}

	log.Debugf("RegroupSubordinates: multi matching %d later subordinates", len(laterSubordinates))
	// As for the laterSubordinates, we'll have to apply pseudo GTID
	laterSubordinates, instance, err, _ := MultiMatchBelow(laterSubordinates, &candidateSubordinate.Key, true, postponedFunctionsContainer)

	operatedSubordinates := append(equalSubordinates, candidateSubordinate)
	operatedSubordinates = append(operatedSubordinates, laterSubordinates...)
	log.Debugf("RegroupSubordinates: starting %d subordinates", len(operatedSubordinates))
	barrier = make(chan *InstanceKey)
	for _, subordinate := range operatedSubordinates {
		subordinate := subordinate
		go func() {
			defer func() { barrier <- &candidateSubordinate.Key }()
			ExecuteOnTopology(func() {
				StartSubordinate(&subordinate.Key)
			})
		}()
	}
	for range operatedSubordinates {
		<-barrier
	}

	log.Debugf("RegroupSubordinates: done")
	AuditOperation("regroup-subordinates", mainKey, fmt.Sprintf("regrouped %+v subordinates below %+v", len(operatedSubordinates), *mainKey))
	// aheadSubordinates are lost (they were ahead in replication as compared to promoted subordinate)
	return aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, instance, err
}

func getMostUpToDateActiveBinlogServer(mainKey *InstanceKey) (mostAdvancedBinlogServer *Instance, binlogServerSubordinates [](*Instance), err error) {
	if binlogServerSubordinates, err = ReadBinlogServerSubordinateInstances(mainKey); err == nil && len(binlogServerSubordinates) > 0 {
		// Pick the most advanced binlog sever that is good to go
		for _, binlogServer := range binlogServerSubordinates {
			if binlogServer.IsLastCheckValid {
				if mostAdvancedBinlogServer == nil {
					mostAdvancedBinlogServer = binlogServer
				}
				if mostAdvancedBinlogServer.ExecBinlogCoordinates.SmallerThan(&binlogServer.ExecBinlogCoordinates) {
					mostAdvancedBinlogServer = binlogServer
				}
			}
		}
	}
	return mostAdvancedBinlogServer, binlogServerSubordinates, err
}

// RegroupSubordinatesPseudoGTIDIncludingSubSubordinatesOfBinlogServers uses Pseugo-GTID to regroup subordinates
// of given instance. The function also drill in to subordinates of binlog servers that are replicating from given instance,
// and other recursive binlog servers, as long as they're in the same binlog-server-family.
func RegroupSubordinatesPseudoGTIDIncludingSubSubordinatesOfBinlogServers(mainKey *InstanceKey, returnSubordinateEvenOnFailureToRegroup bool, onCandidateSubordinateChosen func(*Instance), postponedFunctionsContainer *PostponedFunctionsContainer) ([](*Instance), [](*Instance), [](*Instance), [](*Instance), *Instance, error) {
	// First, handle binlog server issues:
	func() error {
		log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: starting on subordinates of %+v", *mainKey)
		// Find the most up to date binlog server:
		mostUpToDateBinlogServer, binlogServerSubordinates, err := getMostUpToDateActiveBinlogServer(mainKey)
		if err != nil {
			return log.Errore(err)
		}
		if mostUpToDateBinlogServer == nil {
			log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: no binlog server replicates from %+v", *mainKey)
			// No binlog server; proceed as normal
			return nil
		}
		log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: most up to date binlog server of %+v: %+v", *mainKey, mostUpToDateBinlogServer.Key)

		// Find the most up to date candidate subordinate:
		candidateSubordinate, _, _, _, _, err := GetCandidateSubordinate(mainKey, true)
		if err != nil {
			return log.Errore(err)
		}
		if candidateSubordinate == nil {
			log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: no candidate subordinate for %+v", *mainKey)
			// Let the followup code handle that
			return nil
		}
		log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: candidate subordinate of %+v: %+v", *mainKey, candidateSubordinate.Key)

		if candidateSubordinate.ExecBinlogCoordinates.SmallerThan(&mostUpToDateBinlogServer.ExecBinlogCoordinates) {
			log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: candidate subordinate %+v coordinates smaller than binlog server %+v", candidateSubordinate.Key, mostUpToDateBinlogServer.Key)
			// Need to align under binlog server...
			candidateSubordinate, err = Repoint(&candidateSubordinate.Key, &mostUpToDateBinlogServer.Key, GTIDHintDeny)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: repointed candidate subordinate %+v under binlog server %+v", candidateSubordinate.Key, mostUpToDateBinlogServer.Key)
			candidateSubordinate, err = StartSubordinateUntilMainCoordinates(&candidateSubordinate.Key, &mostUpToDateBinlogServer.ExecBinlogCoordinates)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: aligned candidate subordinate %+v under binlog server %+v", candidateSubordinate.Key, mostUpToDateBinlogServer.Key)
			// and move back
			candidateSubordinate, err = Repoint(&candidateSubordinate.Key, mainKey, GTIDHintDeny)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: repointed candidate subordinate %+v under main %+v", candidateSubordinate.Key, *mainKey)
			return nil
		}
		// Either because it _was_ like that, or we _made_ it so,
		// candidate subordinate is as/more up to date than all binlog servers
		for _, binlogServer := range binlogServerSubordinates {
			log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: matching subordinates of binlog server %+v below %+v", binlogServer.Key, candidateSubordinate.Key)
			// Right now sequentially.
			// At this point just do what you can, don't return an error
			MultiMatchSubordinates(&binlogServer.Key, &candidateSubordinate.Key, "")
			log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: done matching subordinates of binlog server %+v below %+v", binlogServer.Key, candidateSubordinate.Key)
		}
		log.Debugf("RegroupSubordinatesIncludingSubSubordinatesOfBinlogServers: done handling binlog regrouping for %+v; will proceed with normal RegroupSubordinates", *mainKey)
		AuditOperation("regroup-subordinates-including-bls", mainKey, fmt.Sprintf("matched subordinates of binlog server subordinates of %+v under %+v", *mainKey, candidateSubordinate.Key))
		return nil
	}()
	// Proceed to normal regroup:
	return RegroupSubordinatesPseudoGTID(mainKey, returnSubordinateEvenOnFailureToRegroup, onCandidateSubordinateChosen, postponedFunctionsContainer)
}

// RegroupSubordinatesGTID will choose a candidate subordinate of a given instance, and ensubordinate its siblings using GTID
func RegroupSubordinatesGTID(mainKey *InstanceKey, returnSubordinateEvenOnFailureToRegroup bool, onCandidateSubordinateChosen func(*Instance)) ([](*Instance), [](*Instance), [](*Instance), *Instance, error) {
	var emptySubordinates [](*Instance)
	candidateSubordinate, aheadSubordinates, equalSubordinates, laterSubordinates, cannotReplicateSubordinates, err := GetCandidateSubordinate(mainKey, true)
	if err != nil {
		if !returnSubordinateEvenOnFailureToRegroup {
			candidateSubordinate = nil
		}
		return emptySubordinates, emptySubordinates, emptySubordinates, candidateSubordinate, err
	}

	if onCandidateSubordinateChosen != nil {
		onCandidateSubordinateChosen(candidateSubordinate)
	}

	subordinatesToMove := append(equalSubordinates, laterSubordinates...)
	log.Debugf("RegroupSubordinatesGTID: working on %d subordinates", len(subordinatesToMove))

	movedSubordinates, unmovedSubordinates, err, _ := moveSubordinatesViaGTID(subordinatesToMove, candidateSubordinate)
	if err != nil {
		log.Errore(err)
	}
	unmovedSubordinates = append(unmovedSubordinates, aheadSubordinates...)
	StartSubordinate(&candidateSubordinate.Key)

	log.Debugf("RegroupSubordinatesGTID: done")
	AuditOperation("regroup-subordinates-gtid", mainKey, fmt.Sprintf("regrouped subordinates of %+v via GTID; promoted %+v", *mainKey, candidateSubordinate.Key))
	return unmovedSubordinates, movedSubordinates, cannotReplicateSubordinates, candidateSubordinate, err
}

// RegroupSubordinatesBinlogServers works on a binlog-servers topology. It picks the most up-to-date BLS and repoints all other
// BLS below it
func RegroupSubordinatesBinlogServers(mainKey *InstanceKey, returnSubordinateEvenOnFailureToRegroup bool) (repointedBinlogServers [](*Instance), promotedBinlogServer *Instance, err error) {
	var binlogServerSubordinates [](*Instance)
	promotedBinlogServer, binlogServerSubordinates, err = getMostUpToDateActiveBinlogServer(mainKey)

	resultOnError := func(err error) ([](*Instance), *Instance, error) {
		if !returnSubordinateEvenOnFailureToRegroup {
			promotedBinlogServer = nil
		}
		return repointedBinlogServers, promotedBinlogServer, err
	}

	if err != nil {
		return resultOnError(err)
	}

	repointedBinlogServers, err, _ = RepointTo(binlogServerSubordinates, &promotedBinlogServer.Key)

	if err != nil {
		return resultOnError(err)
	}
	AuditOperation("regroup-subordinates-bls", mainKey, fmt.Sprintf("regrouped binlog server subordinates of %+v; promoted %+v", *mainKey, promotedBinlogServer.Key))
	return repointedBinlogServers, promotedBinlogServer, nil
}

// RegroupSubordinates is a "smart" method of promoting one subordinate over the others ("promoting" it on top of its siblings)
// This method decides which strategy to use: GTID, Pseudo-GTID, Binlog Servers.
func RegroupSubordinates(mainKey *InstanceKey, returnSubordinateEvenOnFailureToRegroup bool,
	onCandidateSubordinateChosen func(*Instance),
	postponedFunctionsContainer *PostponedFunctionsContainer) (
	aheadSubordinates [](*Instance), equalSubordinates [](*Instance), laterSubordinates [](*Instance), cannotReplicateSubordinates [](*Instance), instance *Instance, err error) {
	//
	var emptySubordinates [](*Instance)

	subordinates, err := ReadSubordinateInstances(mainKey)
	if err != nil {
		return emptySubordinates, emptySubordinates, emptySubordinates, emptySubordinates, instance, err
	}
	if len(subordinates) == 0 {
		return emptySubordinates, emptySubordinates, emptySubordinates, emptySubordinates, instance, err
	}
	if len(subordinates) == 1 {
		return emptySubordinates, emptySubordinates, emptySubordinates, emptySubordinates, subordinates[0], err
	}
	allGTID := true
	allBinlogServers := true
	allPseudoGTID := true
	for _, subordinate := range subordinates {
		if !subordinate.UsingGTID() {
			allGTID = false
		}
		if !subordinate.IsBinlogServer() {
			allBinlogServers = false
		}
		if !subordinate.UsingPseudoGTID {
			allPseudoGTID = false
		}
	}
	if allGTID {
		log.Debugf("RegroupSubordinates: using GTID to regroup subordinates of %+v", *mainKey)
		unmovedSubordinates, movedSubordinates, cannotReplicateSubordinates, candidateSubordinate, err := RegroupSubordinatesGTID(mainKey, returnSubordinateEvenOnFailureToRegroup, onCandidateSubordinateChosen)
		return unmovedSubordinates, emptySubordinates, movedSubordinates, cannotReplicateSubordinates, candidateSubordinate, err
	}
	if allBinlogServers {
		log.Debugf("RegroupSubordinates: using binlog servers to regroup subordinates of %+v", *mainKey)
		movedSubordinates, candidateSubordinate, err := RegroupSubordinatesBinlogServers(mainKey, returnSubordinateEvenOnFailureToRegroup)
		return emptySubordinates, emptySubordinates, movedSubordinates, cannotReplicateSubordinates, candidateSubordinate, err
	}
	if allPseudoGTID {
		log.Debugf("RegroupSubordinates: using Pseudo-GTID to regroup subordinates of %+v", *mainKey)
		return RegroupSubordinatesPseudoGTID(mainKey, returnSubordinateEvenOnFailureToRegroup, onCandidateSubordinateChosen, postponedFunctionsContainer)
	}
	// And, as last resort, we do PseudoGTID & binlog servers
	log.Warningf("RegroupSubordinates: unsure what method to invoke for %+v; trying Pseudo-GTID+Binlog Servers", *mainKey)
	return RegroupSubordinatesPseudoGTIDIncludingSubSubordinatesOfBinlogServers(mainKey, returnSubordinateEvenOnFailureToRegroup, onCandidateSubordinateChosen, postponedFunctionsContainer)
}

// relocateBelowInternal is a protentially recursive function which chooses how to relocate an instance below another.
// It may choose to use Pseudo-GTID, or normal binlog positions, or take advantage of binlog servers,
// or it may combine any of the above in a multi-step operation.
func relocateBelowInternal(instance, other *Instance) (*Instance, error) {
	if canReplicate, err := instance.CanReplicateFrom(other); !canReplicate {
		return instance, log.Errorf("%+v cannot replicate from %+v. Reason: %+v", instance.Key, other.Key, err)
	}
	// simplest:
	if InstanceIsMainOf(other, instance) {
		// already the desired setup.
		return Repoint(&instance.Key, &other.Key, GTIDHintNeutral)
	}
	// Do we have record of equivalent coordinates?
	if !instance.IsBinlogServer() {
		if movedInstance, err := MoveEquivalent(&instance.Key, &other.Key); err == nil {
			return movedInstance, nil
		}
	}
	// Try and take advantage of binlog servers:
	if InstancesAreSiblings(instance, other) && other.IsBinlogServer() {
		return MoveBelow(&instance.Key, &other.Key)
	}
	instanceMain, _, err := ReadInstance(&instance.MainKey)
	if err != nil {
		return instance, err
	}
	if instanceMain != nil && instanceMain.MainKey.Equals(&other.Key) && instanceMain.IsBinlogServer() {
		// Moving to grandparent via binlog server
		return Repoint(&instance.Key, &instanceMain.MainKey, GTIDHintDeny)
	}
	if other.IsBinlogServer() {
		if instanceMain != nil && instanceMain.IsBinlogServer() && InstancesAreSiblings(instanceMain, other) {
			// Special case: this is a binlog server family; we move under the uncle, in one single step
			return Repoint(&instance.Key, &other.Key, GTIDHintDeny)
		}

		// Relocate to its main, then repoint to the binlog server
		otherMain, found, err := ReadInstance(&other.MainKey)
		if err != nil {
			return instance, err
		}
		if !found {
			return instance, log.Errorf("Cannot find main %+v", other.MainKey)
		}
		if !other.IsLastCheckValid {
			return instance, log.Errorf("Binlog server %+v is not reachable. It would take two steps to relocate %+v below it, and I won't even do the first step.", other.Key, instance.Key)
		}

		log.Debugf("Relocating to a binlog server; will first attempt to relocate to the binlog server's main: %+v, and then repoint down", otherMain.Key)
		if _, err := relocateBelowInternal(instance, otherMain); err != nil {
			return instance, err
		}
		return Repoint(&instance.Key, &other.Key, GTIDHintDeny)
	}
	if instance.IsBinlogServer() {
		// Can only move within the binlog-server family tree
		// And these have been covered just now: move up from a main binlog server, move below a binling binlog server.
		// sure, the family can be more complex, but we keep these operations atomic
		return nil, log.Errorf("Relocating binlog server %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
	}
	// Next, try GTID
	if _, _, canMove := canMoveViaGTID(instance, other); canMove {
		return moveInstanceBelowViaGTID(instance, other)
	}

	// Next, try Pseudo-GTID
	if instance.UsingPseudoGTID && other.UsingPseudoGTID {
		// We prefer PseudoGTID to anything else because, while it takes longer to run, it does not issue
		// a STOP SLAVE on any server other than "instance" itself.
		instance, _, err := MatchBelow(&instance.Key, &other.Key, true)
		return instance, err
	}
	// No Pseudo-GTID; cehck simple binlog file/pos operations:
	if InstancesAreSiblings(instance, other) {
		// If comaining, only move below if it's read-only
		if !other.IsCoMain || other.ReadOnly {
			return MoveBelow(&instance.Key, &other.Key)
		}
	}
	// See if we need to MoveUp
	if instanceMain != nil && instanceMain.MainKey.Equals(&other.Key) {
		// Moving to grandparent--handles co-maining writable case
		return MoveUp(&instance.Key)
	}
	if instanceMain != nil && instanceMain.IsBinlogServer() {
		// Break operation into two: move (repoint) up, then continue
		if _, err := MoveUp(&instance.Key); err != nil {
			return instance, err
		}
		return relocateBelowInternal(instance, other)
	}
	// Too complex
	return nil, log.Errorf("Relocating %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
}

// RelocateBelow will attempt moving instance indicated by instanceKey below another instance.
// Orchestrator will try and figure out the best way to relocate the server. This could span normal
// binlog-position, pseudo-gtid, repointing, binlog servers...
func RelocateBelow(instanceKey, otherKey *InstanceKey) (*Instance, error) {
	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return instance, log.Errorf("Error reading %+v", *instanceKey)
	}
	other, found, err := ReadInstance(otherKey)
	if err != nil || !found {
		return instance, log.Errorf("Error reading %+v", *otherKey)
	}
	instance, err = relocateBelowInternal(instance, other)
	if err == nil {
		AuditOperation("relocate-below", instanceKey, fmt.Sprintf("relocated %+v below %+v", *instanceKey, *otherKey))
	}
	return instance, err
}

// relocateSubordinatesInternal is a protentially recursive function which chooses how to relocate
// subordinates of an instance below another.
// It may choose to use Pseudo-GTID, or normal binlog positions, or take advantage of binlog servers,
// or it may combine any of the above in a multi-step operation.
func relocateSubordinatesInternal(subordinates [](*Instance), instance, other *Instance) ([](*Instance), error, []error) {
	errs := []error{}
	var err error
	// simplest:
	if instance.Key.Equals(&other.Key) {
		// already the desired setup.
		return RepointTo(subordinates, &other.Key)
	}
	// Try and take advantage of binlog servers:
	if InstanceIsMainOf(other, instance) && instance.IsBinlogServer() {
		// Up from a binlog server
		return RepointTo(subordinates, &other.Key)
	}
	if InstanceIsMainOf(instance, other) && other.IsBinlogServer() {
		// Down under a binlog server
		return RepointTo(subordinates, &other.Key)
	}
	if InstancesAreSiblings(instance, other) && instance.IsBinlogServer() && other.IsBinlogServer() {
		// Between siblings
		return RepointTo(subordinates, &other.Key)
	}
	if other.IsBinlogServer() {
		// Relocate to binlog server's parent (recursive call), then repoint down
		otherMain, found, err := ReadInstance(&other.MainKey)
		if err != nil || !found {
			return nil, err, errs
		}
		subordinates, err, errs = relocateSubordinatesInternal(subordinates, instance, otherMain)
		if err != nil {
			return subordinates, err, errs
		}

		return RepointTo(subordinates, &other.Key)
	}
	// GTID
	{
		movedSubordinates, unmovedSubordinates, err, errs := moveSubordinatesViaGTID(subordinates, other)

		if len(movedSubordinates) == len(subordinates) {
			// Moved (or tried moving) everything via GTID
			return movedSubordinates, err, errs
		} else if len(movedSubordinates) > 0 {
			// something was moved via GTID; let's try further on
			return relocateSubordinatesInternal(unmovedSubordinates, instance, other)
		}
		// Otherwise nothing was moved via GTID. Maybe we don't have any GTIDs, we continue.
	}

	// Pseudo GTID
	if other.UsingPseudoGTID {
		// Which subordinates are using Pseudo GTID?
		var pseudoGTIDSubordinates [](*Instance)
		for _, subordinate := range subordinates {
			if subordinate.UsingPseudoGTID {
				pseudoGTIDSubordinates = append(pseudoGTIDSubordinates, subordinate)
			}
		}
		pseudoGTIDSubordinates, _, err, errs = MultiMatchBelow(pseudoGTIDSubordinates, &other.Key, false, nil)
		return pseudoGTIDSubordinates, err, errs
	}

	// Normal binlog file:pos
	if InstanceIsMainOf(other, instance) {
		// moveUpSubordinates -- but not supporting "subordinates" argument at this time.
	}

	// Too complex
	return nil, log.Errorf("Relocating %+v subordinates of %+v below %+v turns to be too complex; please do it manually", len(subordinates), instance.Key, other.Key), errs
}

// RelocateSubordinates will attempt moving subordinates of an instance indicated by instanceKey below another instance.
// Orchestrator will try and figure out the best way to relocate the servers. This could span normal
// binlog-position, pseudo-gtid, repointing, binlog servers...
func RelocateSubordinates(instanceKey, otherKey *InstanceKey, pattern string) (subordinates [](*Instance), other *Instance, err error, errs []error) {

	instance, found, err := ReadInstance(instanceKey)
	if err != nil || !found {
		return subordinates, other, log.Errorf("Error reading %+v", *instanceKey), errs
	}
	other, found, err = ReadInstance(otherKey)
	if err != nil || !found {
		return subordinates, other, log.Errorf("Error reading %+v", *otherKey), errs
	}

	subordinates, err = ReadSubordinateInstances(instanceKey)
	if err != nil {
		return subordinates, other, err, errs
	}
	subordinates = RemoveInstance(subordinates, otherKey)
	subordinates = filterInstancesByPattern(subordinates, pattern)
	if len(subordinates) == 0 {
		// Nothing to do
		return subordinates, other, nil, errs
	}
	subordinates, err, errs = relocateSubordinatesInternal(subordinates, instance, other)

	if err == nil {
		AuditOperation("relocate-subordinates", instanceKey, fmt.Sprintf("relocated %+v subordinates of %+v below %+v", len(subordinates), *instanceKey, *otherKey))
	}
	return subordinates, other, err, errs
}
