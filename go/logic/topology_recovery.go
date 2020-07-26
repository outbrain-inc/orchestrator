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

package logic

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/outbrain/golib/log"
	"github.com/outbrain/orchestrator/go/attributes"
	"github.com/outbrain/orchestrator/go/config"
	"github.com/outbrain/orchestrator/go/inst"
	"github.com/outbrain/orchestrator/go/os"
	"github.com/outbrain/orchestrator/go/process"
	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
)

// BlockedTopologyRecovery represents an entry in the blocked_topology_recovery table
type BlockedTopologyRecovery struct {
	FailedInstanceKey    inst.InstanceKey
	ClusterName          string
	Analysis             inst.AnalysisCode
	LastBlockedTimestamp string
	BlockingRecoveryId   int64
}

// TopologyRecovery represents an entry in the topology_recovery table
type TopologyRecovery struct {
	inst.PostponedFunctionsContainer

	Id                        int64
	AnalysisEntry             inst.ReplicationAnalysis
	SuccessorKey              *inst.InstanceKey
	SuccessorAlias            string
	IsActive                  bool
	IsSuccessful              bool
	LostSubordinates                inst.InstanceKeyMap
	ParticipatingInstanceKeys inst.InstanceKeyMap
	AllErrors                 []string
	RecoveryStartTimestamp    string
	RecoveryEndTimestamp      string
	ProcessingNodeHostname    string
	ProcessingNodeToken       string
	PostponedFunctions        [](func() error)
	Acknowledged              bool
	AcknowledgedAt            string
	AcknowledgedBy            string
	AcknowledgedComment       string
	LastDetectionId           int64
	RelatedRecoveryId         int64
}

func NewTopologyRecovery(replicationAnalysis inst.ReplicationAnalysis) *TopologyRecovery {
	topologyRecovery := &TopologyRecovery{}
	topologyRecovery.AnalysisEntry = replicationAnalysis
	topologyRecovery.SuccessorKey = nil
	topologyRecovery.LostSubordinates = *inst.NewInstanceKeyMap()
	topologyRecovery.ParticipatingInstanceKeys = *inst.NewInstanceKeyMap()
	topologyRecovery.AllErrors = []string{}
	topologyRecovery.PostponedFunctions = [](func() error){}
	return topologyRecovery
}

func (this *TopologyRecovery) AddError(err error) error {
	if err != nil {
		this.AllErrors = append(this.AllErrors, err.Error())
	}
	return err
}

func (this *TopologyRecovery) AddErrors(errs []error) {
	for _, err := range errs {
		this.AddError(err)
	}
}

type MainRecoveryType string

const (
	MainRecoveryGTID         MainRecoveryType = "MainRecoveryGTID"
	MainRecoveryPseudoGTID                      = "MainRecoveryPseudoGTID"
	MainRecoveryBinlogServer                    = "MainRecoveryBinlogServer"
)

var emptySubordinatesList [](*inst.Instance)

var emergencyReadTopologyInstanceMap = cache.New(time.Duration(config.Config.InstancePollSeconds)*time.Second, time.Second)

// InstancesByCountSubordinates sorts instances by umber of subordinates, descending
type InstancesByCountSubordinates [](*inst.Instance)

func (this InstancesByCountSubordinates) Len() int      { return len(this) }
func (this InstancesByCountSubordinates) Swap(i, j int) { this[i], this[j] = this[j], this[i] }
func (this InstancesByCountSubordinates) Less(i, j int) bool {
	if len(this[i].SubordinateHosts) == len(this[j].SubordinateHosts) {
		// Secondary sorting: prefer more advanced subordinates
		return !this[i].ExecBinlogCoordinates.SmallerThan(&this[j].ExecBinlogCoordinates)
	}
	return len(this[i].SubordinateHosts) < len(this[j].SubordinateHosts)
}

var recoverDeadMainCounter = metrics.NewCounter()
var recoverDeadMainSuccessCounter = metrics.NewCounter()
var recoverDeadMainFailureCounter = metrics.NewCounter()
var recoverDeadIntermediateMainCounter = metrics.NewCounter()
var recoverDeadIntermediateMainSuccessCounter = metrics.NewCounter()
var recoverDeadIntermediateMainFailureCounter = metrics.NewCounter()
var recoverDeadCoMainCounter = metrics.NewCounter()
var recoverDeadCoMainSuccessCounter = metrics.NewCounter()
var recoverDeadCoMainFailureCounter = metrics.NewCounter()
var recoverUnreachableMainWithStaleSubordinatesCounter = metrics.NewCounter()
var recoverUnreachableMainWithStaleSubordinatesSuccessCounter = metrics.NewCounter()
var recoverUnreachableMainWithStaleSubordinatesFailureCounter = metrics.NewCounter()

func init() {
	metrics.Register("recover.dead_main.start", recoverDeadMainCounter)
	metrics.Register("recover.dead_main.success", recoverDeadMainSuccessCounter)
	metrics.Register("recover.dead_main.fail", recoverDeadMainFailureCounter)
	metrics.Register("recover.dead_intermediate_main.start", recoverDeadIntermediateMainCounter)
	metrics.Register("recover.dead_intermediate_main.success", recoverDeadIntermediateMainSuccessCounter)
	metrics.Register("recover.dead_intermediate_main.fail", recoverDeadIntermediateMainFailureCounter)
	metrics.Register("recover.dead_co_main.start", recoverDeadCoMainCounter)
	metrics.Register("recover.dead_co_main.success", recoverDeadCoMainSuccessCounter)
	metrics.Register("recover.dead_co_main.fail", recoverDeadCoMainFailureCounter)
	metrics.Register("recover.unreach_main_stale_subordinates.start", recoverUnreachableMainWithStaleSubordinatesCounter)
	metrics.Register("recover.unreach_main_stale_subordinates.success", recoverUnreachableMainWithStaleSubordinatesSuccessCounter)
	metrics.Register("recover.unreach_main_stale_subordinates.fail", recoverUnreachableMainWithStaleSubordinatesFailureCounter)
}

// replaceCommandPlaceholders replaces agreed-upon placeholders with analysis data
func replaceCommandPlaceholders(command string, topologyRecovery *TopologyRecovery) string {
	analysisEntry := &topologyRecovery.AnalysisEntry
	command = strings.Replace(command, "{failureType}", string(analysisEntry.Analysis), -1)
	command = strings.Replace(command, "{failureDescription}", analysisEntry.Description, -1)
	command = strings.Replace(command, "{failedHost}", analysisEntry.AnalyzedInstanceKey.Hostname, -1)
	command = strings.Replace(command, "{failedPort}", fmt.Sprintf("%d", analysisEntry.AnalyzedInstanceKey.Port), -1)
	command = strings.Replace(command, "{failureCluster}", analysisEntry.ClusterDetails.ClusterName, -1)
	command = strings.Replace(command, "{failureClusterAlias}", analysisEntry.ClusterDetails.ClusterAlias, -1)
	command = strings.Replace(command, "{failureClusterDomain}", analysisEntry.ClusterDetails.ClusterDomain, -1)
	command = strings.Replace(command, "{countSubordinates}", fmt.Sprintf("%d", analysisEntry.CountSubordinates), -1)
	command = strings.Replace(command, "{isDowntimed}", fmt.Sprint(analysisEntry.IsDowntimed), -1)
	command = strings.Replace(command, "{autoMainRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedMainRecovery), -1)
	command = strings.Replace(command, "{autoIntermediateMainRecovery}", fmt.Sprint(analysisEntry.ClusterDetails.HasAutomatedIntermediateMainRecovery), -1)
	command = strings.Replace(command, "{orchestratorHost}", process.ThisHostname, -1)

	command = strings.Replace(command, "{isSuccessful}", fmt.Sprint(topologyRecovery.SuccessorKey != nil), -1)
	if topologyRecovery.SuccessorKey != nil {
		command = strings.Replace(command, "{successorHost}", topologyRecovery.SuccessorKey.Hostname, -1)
		command = strings.Replace(command, "{successorPort}", fmt.Sprintf("%d", topologyRecovery.SuccessorKey.Port), -1)
		// As long as SucesssorKey != nil, we replace {successorAlias}.
		// If SucessorAlias is "", it's fine. We'll replace {successorAlias} with "".
		command = strings.Replace(command, "{successorAlias}", topologyRecovery.SuccessorAlias, -1)
	}

	command = strings.Replace(command, "{lostSubordinates}", topologyRecovery.LostSubordinates.ToCommaDelimitedList(), -1)
	command = strings.Replace(command, "{subordinateHosts}", analysisEntry.SubordinateHosts.ToCommaDelimitedList(), -1)

	return command
}

// executeProcesses executes a list of processes
func executeProcesses(processes []string, description string, topologyRecovery *TopologyRecovery, failOnError bool) error {
	var err error
	for _, command := range processes {
		command := replaceCommandPlaceholders(command, topologyRecovery)

		if cmdErr := os.CommandRun(command); cmdErr == nil {
			log.Infof("Executed %s command: %s", description, command)
		} else {
			if err == nil {
				// Note first error
				err = cmdErr
			}
			log.Errorf("Failed to execute %s command: %s", description, command)
			if failOnError {
				return err
			}
		}
	}
	return err
}

func recoverDeadMainInBinlogServerTopology(topologyRecovery *TopologyRecovery) (promotedSubordinate *inst.Instance, err error) {
	failedMainKey := &topologyRecovery.AnalysisEntry.AnalyzedInstanceKey

	var promotedBinlogServer *inst.Instance

	_, promotedBinlogServer, err = inst.RegroupSubordinatesBinlogServers(failedMainKey, true)
	if err != nil {
		return nil, log.Errore(err)
	}
	promotedBinlogServer, err = inst.StopSubordinate(&promotedBinlogServer.Key)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	// Find candidate subordinate
	promotedSubordinate, err = inst.GetCandidateSubordinateOfBinlogServerTopology(&promotedBinlogServer.Key)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	// Align it with binlog server coordinates
	promotedSubordinate, err = inst.StopSubordinate(&promotedSubordinate.Key)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	promotedSubordinate, err = inst.StartSubordinateUntilMainCoordinates(&promotedSubordinate.Key, &promotedBinlogServer.ExecBinlogCoordinates)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	promotedSubordinate, err = inst.StopSubordinate(&promotedSubordinate.Key)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	// Detach, flush binary logs forward
	promotedSubordinate, err = inst.ResetSubordinate(&promotedSubordinate.Key)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	promotedSubordinate, err = inst.FlushBinaryLogsTo(&promotedSubordinate.Key, promotedBinlogServer.ExecBinlogCoordinates.LogFile)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	promotedSubordinate, err = inst.FlushBinaryLogs(&promotedSubordinate.Key, 1)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	promotedSubordinate, err = inst.PurgeBinaryLogsToCurrent(&promotedSubordinate.Key)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	// Reconnect binlog servers to promoted subordinate (now main):
	promotedBinlogServer, err = inst.SkipToNextBinaryLog(&promotedBinlogServer.Key)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}
	promotedBinlogServer, err = inst.Repoint(&promotedBinlogServer.Key, &promotedSubordinate.Key, inst.GTIDHintDeny)
	if err != nil {
		return nil, log.Errore(err)
	}

	func() {
		// Move binlog server subordinates up to replicate from main.
		// This can only be done once a BLS has skipped to the next binlog
		// We postpone this operation. The main is already promoted and we're happy.
		binlogServerSubordinates, err := inst.ReadBinlogServerSubordinateInstances(&promotedBinlogServer.Key)
		if err != nil {
			return
		}
		maxBinlogServersToPromote := 3
		for i, binlogServerSubordinate := range binlogServerSubordinates {
			binlogServerSubordinate := binlogServerSubordinate
			if i >= maxBinlogServersToPromote {
				return
			}
			postponedFunction := func() error {
				binlogServerSubordinate, err := inst.StopSubordinate(&binlogServerSubordinate.Key)
				if err != nil {
					return err
				}
				// Make sure the BLS has the "next binlog" -- the one the main flushed & purged to. Otherwise the BLS
				// will request a binlog the main does not have
				if binlogServerSubordinate.ExecBinlogCoordinates.SmallerThan(&promotedBinlogServer.ExecBinlogCoordinates) {
					binlogServerSubordinate, err = inst.StartSubordinateUntilMainCoordinates(&binlogServerSubordinate.Key, &promotedBinlogServer.ExecBinlogCoordinates)
					if err != nil {
						return err
					}
				}
				_, err = inst.Repoint(&binlogServerSubordinate.Key, &promotedSubordinate.Key, inst.GTIDHintDeny)
				return err
			}
			topologyRecovery.AddPostponedFunction(postponedFunction)
		}
	}()

	return promotedSubordinate, err
}

// RecoverDeadMain recovers a dead main, complete logic inside
func RecoverDeadMain(topologyRecovery *TopologyRecovery, skipProcesses bool) (promotedSubordinate *inst.Instance, lostSubordinates [](*inst.Instance), err error) {
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	var cannotReplicateSubordinates [](*inst.Instance)

	inst.AuditOperation("recover-dead-main", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, lostSubordinates, topologyRecovery.AddError(err)
		}
	}

	log.Debugf("topology_recovery: RecoverDeadMain: will recover %+v", *failedInstanceKey)

	var mainRecoveryType MainRecoveryType = MainRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		mainRecoveryType = MainRecoveryGTID
	} else if analysisEntry.BinlogServerImmediateTopology {
		mainRecoveryType = MainRecoveryBinlogServer
	}
	log.Debugf("topology_recovery: RecoverDeadMain: mainRecoveryType=%+v", mainRecoveryType)

	switch mainRecoveryType {
	case MainRecoveryGTID:
		{
			lostSubordinates, _, cannotReplicateSubordinates, promotedSubordinate, err = inst.RegroupSubordinatesGTID(failedInstanceKey, true, nil)
		}
	case MainRecoveryPseudoGTID:
		{
			lostSubordinates, _, _, cannotReplicateSubordinates, promotedSubordinate, err = inst.RegroupSubordinatesPseudoGTIDIncludingSubSubordinatesOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer)
		}
	case MainRecoveryBinlogServer:
		{
			promotedSubordinate, err = recoverDeadMainInBinlogServerTopology(topologyRecovery)
		}
	}
	topologyRecovery.AddError(err)
	lostSubordinates = append(lostSubordinates, cannotReplicateSubordinates...)

	if promotedSubordinate != nil && len(lostSubordinates) > 0 && config.Config.DetachLostSubordinatesAfterMainFailover {
		postponedFunction := func() error {
			log.Debugf("topology_recovery: - RecoverDeadMain: lost %+v subordinates during recovery process; detaching them", len(lostSubordinates))
			for _, subordinate := range lostSubordinates {
				subordinate := subordinate
				inst.DetachSubordinateOperation(&subordinate.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction)
	}
	if config.Config.MainFailoverLostInstancesDowntimeMinutes > 0 {
		postponedFunction := func() error {
			inst.BeginDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, config.Config.MainFailoverLostInstancesDowntimeMinutes*60)
			for _, subordinate := range lostSubordinates {
				subordinate := subordinate
				inst.BeginDowntime(&subordinate.Key, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, config.Config.MainFailoverLostInstancesDowntimeMinutes*60)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction)
	}

	if promotedSubordinate == nil {
		inst.AuditOperation("recover-dead-main", failedInstanceKey, "Failure: no subordinate promoted.")
	} else {
		inst.AuditOperation("recover-dead-main", failedInstanceKey, fmt.Sprintf("promoted subordinate: %+v", promotedSubordinate.Key))
	}
	return promotedSubordinate, lostSubordinates, err
}

// replacePromotedSubordinateWithCandidate is called after an intermediate main has died and been replaced by some promotedSubordinate.
// But, is there an even better subordinate to promote?
// if candidateInstanceKey is given, then it is forced to be promoted over the promotedSubordinate
// Otherwise, search for the best to promote!
func replacePromotedSubordinateWithCandidate(deadInstanceKey *inst.InstanceKey, promotedSubordinate *inst.Instance, candidateInstanceKey *inst.InstanceKey) (*inst.Instance, error) {
	candidateSubordinates, _ := inst.ReadClusterCandidateInstances(promotedSubordinate.ClusterName)
	// So we've already promoted a subordinate.
	// However, can we improve on our choice? Are there any subordinates marked with "is_candidate"?
	// Maybe we actually promoted such a subordinate. Does that mean we should keep it?
	// The current logic is:
	// - 1. we prefer to promote a "is_candidate" which is in the same DC & env as the dead intermediate main (or do nothing if the promtoed subordinate is such one)
	// - 2. we prefer to promote a "is_candidate" which is in the same DC & env as the promoted subordinate (or do nothing if the promtoed subordinate is such one)
	// - 3. keep to current choice
	log.Infof("topology_recovery: checking if should replace promoted subordinate with a better candidate")
	if candidateInstanceKey == nil {
		if deadInstance, _, err := inst.ReadInstance(deadInstanceKey); err == nil && deadInstance != nil {
			for _, candidateSubordinate := range candidateSubordinates {
				if promotedSubordinate.Key.Equals(&candidateSubordinate.Key) &&
					promotedSubordinate.DataCenter == deadInstance.DataCenter &&
					promotedSubordinate.PhysicalEnvironment == deadInstance.PhysicalEnvironment {
					// Seems like we promoted a candidate in the same DC & ENV as dead IM! Ideal! We're happy!
					log.Infof("topology_recovery: promoted subordinate %+v is the ideal candidate", promotedSubordinate.Key)
					return promotedSubordinate, nil
				}
			}
		}
	}
	// We didn't pick the ideal candidate; let's see if we can replace with a candidate from same DC and ENV
	if candidateInstanceKey == nil {
		// Try a candidate subordinate that is in same DC & env as the dead instance
		if deadInstance, _, err := inst.ReadInstance(deadInstanceKey); err == nil && deadInstance != nil {
			for _, candidateSubordinate := range candidateSubordinates {
				if candidateSubordinate.DataCenter == deadInstance.DataCenter &&
					candidateSubordinate.PhysicalEnvironment == deadInstance.PhysicalEnvironment &&
					candidateSubordinate.MainKey.Equals(&promotedSubordinate.Key) {
					// This would make a great candidate
					candidateInstanceKey = &candidateSubordinate.Key
					log.Debugf("topology_recovery: no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as failed instance", promotedSubordinate.Key, candidateSubordinate.Key)
				}
			}
		}
	}
	if candidateInstanceKey == nil {
		// We cannot find a candidate in same DC and ENV as dead main
		for _, candidateSubordinate := range candidateSubordinates {
			if promotedSubordinate.Key.Equals(&candidateSubordinate.Key) {
				// Seems like we promoted a candidate subordinate (though not in same DC and ENV as dead main). Good enough.
				// No further action required.
				log.Infof("topology_recovery: promoted subordinate %+v is a good candidate", promotedSubordinate.Key)
				return promotedSubordinate, nil
			}
		}
	}
	// Still nothing?
	if candidateInstanceKey == nil {
		// Try a candidate subordinate that is in same DC & env as the promoted subordinate (our promoted subordinate is not an "is_candidate")
		for _, candidateSubordinate := range candidateSubordinates {
			if promotedSubordinate.DataCenter == candidateSubordinate.DataCenter &&
				promotedSubordinate.PhysicalEnvironment == candidateSubordinate.PhysicalEnvironment &&
				candidateSubordinate.MainKey.Equals(&promotedSubordinate.Key) {
				// OK, better than nothing
				candidateInstanceKey = &candidateSubordinate.Key
				log.Debugf("topology_recovery: no candidate was offered for %+v but orchestrator picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedSubordinate.Key, candidateSubordinate.Key)
			}
		}
	}

	// So do we have a candidate?
	if candidateInstanceKey == nil {
		// Found nothing. Stick with promoted subordinate
		return promotedSubordinate, nil
	}
	if promotedSubordinate.Key.Equals(candidateInstanceKey) {
		// Sanity. It IS the candidate, nothing to promote...
		return promotedSubordinate, nil
	}

	// Try and promote suggested candidate, if applicable and possible
	log.Debugf("topology_recovery: promoted instance %+v is not the suggested candidate %+v. Will see what can be done", promotedSubordinate.Key, *candidateInstanceKey)

	candidateInstance, _, err := inst.ReadInstance(candidateInstanceKey)
	if err != nil {
		return promotedSubordinate, log.Errore(err)
	}

	if candidateInstance.MainKey.Equals(&promotedSubordinate.Key) {
		log.Debugf("topology_recovery: suggested candidate %+v is subordinate of promoted instance %+v. Will try and ensubordinate its main", *candidateInstanceKey, promotedSubordinate.Key)
		candidateInstance, err = inst.EnsubordinateMain(&candidateInstance.Key)
		if err != nil {
			return promotedSubordinate, log.Errore(err)
		}
		log.Debugf("topology_recovery: success promoting %+v over %+v", *candidateInstanceKey, promotedSubordinate.Key)
		return candidateInstance, nil
	}

	log.Debugf("topology_recovery: could not manage to promoted suggested candidate %+v", *candidateInstanceKey)
	return promotedSubordinate, nil
}

// checkAndRecoverDeadMain checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadMain(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMainRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		log.Debugf("topology_recovery: found an active or recent recovery on %+v. Will not issue another RecoverDeadMain.", analysisEntry.AnalyzedInstanceKey)
		return false, nil, err
	}

	// That's it! We must do recovery!
	log.Debugf("topology_recovery: will handle DeadMain event on %+v", analysisEntry.ClusterDetails.ClusterName)
	recoverDeadMainCounter.Inc(1)
	promotedSubordinate, lostSubordinates, err := RecoverDeadMain(topologyRecovery, skipProcesses)
	topologyRecovery.LostSubordinates.AddInstances(lostSubordinates)

	if promotedSubordinate != nil {
		promotedSubordinate, err = replacePromotedSubordinateWithCandidate(&analysisEntry.AnalyzedInstanceKey, promotedSubordinate, candidateInstanceKey)
		topologyRecovery.AddError(err)
	}
	// And this is the end; whether successful or not, we're done.
	ResolveRecovery(topologyRecovery, promotedSubordinate)
	if promotedSubordinate != nil {
		// Success!
		recoverDeadMainSuccessCounter.Inc(1)

		if config.Config.ApplyMySQLPromotionAfterMainFailover {
			log.Debugf("topology_recovery: - RecoverDeadMain: will apply MySQL changes to promoted main")
			inst.ResetSubordinateOperation(&promotedSubordinate.Key)
			inst.SetReadOnly(&promotedSubordinate.Key, false)
		}
		if !skipProcesses {
			// Execute post main-failover processes
			executeProcesses(config.Config.PostMainFailoverProcesses, "PostMainFailoverProcesses", topologyRecovery, false)
		}

		if config.Config.MainFailoverDetachSubordinateMainHost {
			postponedFunction := func() error {
				log.Debugf("topology_recovery: - RecoverDeadMain: detaching main host on promoted main")
				inst.DetachSubordinateMainHost(&promotedSubordinate.Key)
				return nil
			}
			topologyRecovery.AddPostponedFunction(postponedFunction)
		}
		postponedFunction := func() error {
			log.Debugf("topology_recovery: - RecoverDeadMain: updating cluster_alias")
			inst.ReplaceAliasClusterName(analysisEntry.AnalyzedInstanceKey.StringCode(), promotedSubordinate.Key.StringCode())
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction)

		attributes.SetGeneralAttribute(analysisEntry.ClusterDetails.ClusterDomain, promotedSubordinate.Key.StringCode())
	} else {
		recoverDeadMainFailureCounter.Inc(1)
	}

	return true, topologyRecovery, err
}

// isGeneralyValidAsCandidateSiblingOfIntermediateMain sees that basic server configuration and state are valid
func isGeneralyValidAsCandidateSiblingOfIntermediateMain(sibling *inst.Instance) bool {
	if !sibling.LogBinEnabled {
		return false
	}
	if !sibling.LogSubordinateUpdatesEnabled {
		return false
	}
	if !sibling.SubordinateRunning() {
		return false
	}
	if !sibling.IsLastCheckValid {
		return false
	}
	return true
}

// isValidAsCandidateSiblingOfIntermediateMain checks to see that the given sibling is capable to take over instance's subordinates
func isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance *inst.Instance, sibling *inst.Instance) bool {
	if sibling.Key.Equals(&intermediateMainInstance.Key) {
		// same instance
		return false
	}
	if !isGeneralyValidAsCandidateSiblingOfIntermediateMain(sibling) {
		return false
	}
	if sibling.HasReplicationFilters != intermediateMainInstance.HasReplicationFilters {
		return false
	}
	if sibling.IsBinlogServer() != intermediateMainInstance.IsBinlogServer() {
		// When both are binlog servers, failover is trivial.
		// When failed IM is binlog server, its sibling is still valid, but we catually prefer to just repoint the subordinate up -- simplest!
		return false
	}
	if sibling.ExecBinlogCoordinates.SmallerThan(&intermediateMainInstance.ExecBinlogCoordinates) {
		return false
	}
	return true
}

// GetCandidateSiblingOfIntermediateMain chooses the best sibling of a dead intermediate main
// to whom the IM's subordinates can be moved.
func GetCandidateSiblingOfIntermediateMain(intermediateMainInstance *inst.Instance) (*inst.Instance, error) {

	siblings, err := inst.ReadSubordinateInstances(&intermediateMainInstance.MainKey)
	if err != nil {
		return nil, err
	}
	if len(siblings) <= 1 {
		return nil, log.Errorf("topology_recovery: no siblings found for %+v", intermediateMainInstance.Key)
	}

	sort.Sort(sort.Reverse(InstancesByCountSubordinates(siblings)))

	// In the next series of steps we attempt to return a good replacement.
	// None of the below attempts is sure to pick a winning server. Perhaps picked server is not enough up-todate -- but
	// this has small likelihood in the general case, and, well, it's an attempt. It's a Plan A, but we have Plan B & C if this fails.

	// At first, we try to return an "is_candidate" server in same dc & env
	log.Infof("topology_recovery: searching for the best candidate sibling of dead intermediate main")
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance, sibling) &&
			sibling.IsCandidate &&
			sibling.DataCenter == intermediateMainInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMainInstance.PhysicalEnvironment {
			log.Infof("topology_recovery: found %+v as the ideal candidate", sibling.Key)
			return sibling, nil
		}
	}
	// Go for something else in the same DC & ENV
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance, sibling) &&
			sibling.DataCenter == intermediateMainInstance.DataCenter &&
			sibling.PhysicalEnvironment == intermediateMainInstance.PhysicalEnvironment {
			log.Infof("topology_recovery: found %+v as a replacement in same dc & environment", sibling.Key)
			return sibling, nil
		}
	}
	// Nothing in same DC & env, let's just go for some is_candidate
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance, sibling) && sibling.IsCandidate {
			log.Infof("topology_recovery: found %+v as a good candidate", sibling.Key)
			return sibling, nil
		}
	}
	// Havent found an "is_candidate". Just whatever is valid.
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMain(intermediateMainInstance, sibling) {
			log.Infof("topology_recovery: found %+v as a replacement", sibling.Key)
			return sibling, nil
		}
	}
	return nil, log.Errorf("topology_recovery: cannot find candidate sibling of %+v", intermediateMainInstance.Key)
}

// RecoverDeadIntermediateMain performs intermediate main recovery; complete logic inside
func RecoverDeadIntermediateMain(topologyRecovery *TopologyRecovery, skipProcesses bool) (successorInstance *inst.Instance, err error) {
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	recoveryResolved := false

	inst.AuditOperation("recover-dead-intermediate-main", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	intermediateMainInstance, _, err := inst.ReadInstance(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}
	// Find possible candidate
	candidateSiblingOfIntermediateMain, err := GetCandidateSiblingOfIntermediateMain(intermediateMainInstance)
	relocateSubordinatesToCandidateSibling := func() {
		if candidateSiblingOfIntermediateMain == nil {
			return
		}
		// We have a candidate
		log.Debugf("topology_recovery: - RecoverDeadIntermediateMain: will attempt a candidate intermediate main: %+v", candidateSiblingOfIntermediateMain.Key)
		relocatedSubordinates, candidateSibling, err, errs := inst.RelocateSubordinates(failedInstanceKey, &candidateSiblingOfIntermediateMain.Key, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(candidateSiblingOfIntermediateMain.Key)

		if len(relocatedSubordinates) == 0 {
			log.Debugf("topology_recovery: - RecoverDeadIntermediateMain: failed to move any subordinate to candidate intermediate main (%+v)", candidateSibling.Key)
			return
		}
		if err != nil || len(errs) > 0 {
			log.Debugf("topology_recovery: - RecoverDeadIntermediateMain: move to candidate intermediate main (%+v) did not complete: %+v", candidateSibling.Key, err)
			return
		}
		if err == nil {
			recoveryResolved = true
			successorInstance = candidateSibling

			inst.AuditOperation("recover-dead-intermediate-main", failedInstanceKey, fmt.Sprintf("Relocated %d subordinates under candidate sibling: %+v; %d errors: %+v", len(relocatedSubordinates), candidateSibling.Key, len(errs), errs))
		}
	}
	// Plan A: find a replacement intermediate main in same Data Center
	if candidateSiblingOfIntermediateMain != nil && candidateSiblingOfIntermediateMain.DataCenter == intermediateMainInstance.DataCenter {
		relocateSubordinatesToCandidateSibling()
	}
	if !recoveryResolved {
		log.Debugf("topology_recovery: - RecoverDeadIntermediateMain: will next attempt regrouping of subordinates")
		// Plan B: regroup (we wish to reduce cross-DC replication streams)
		_, _, _, _, regroupPromotedSubordinate, err := inst.RegroupSubordinates(failedInstanceKey, true, nil, nil)
		if err != nil {
			topologyRecovery.AddError(err)
			log.Debugf("topology_recovery: - RecoverDeadIntermediateMain: regroup failed on: %+v", err)
		}
		if regroupPromotedSubordinate != nil {
			topologyRecovery.ParticipatingInstanceKeys.AddKey(regroupPromotedSubordinate.Key)
		}
		// Plan C: try replacement intermediate main in other DC...
		if candidateSiblingOfIntermediateMain != nil && candidateSiblingOfIntermediateMain.DataCenter != intermediateMainInstance.DataCenter {
			log.Debugf("topology_recovery: - RecoverDeadIntermediateMain: will next attempt relocating to another DC server")
			relocateSubordinatesToCandidateSibling()
		}
	}
	if !recoveryResolved {
		// Do we still have leftovers? Some subordinates couldn't move? Couldn't regroup? Only left with regroup's resulting leader?
		// nothing moved?
		// We don't care much if regroup made it or not. We prefer that it made it, in whcih case we only need to relocate up
		// one subordinate, but the operation is still valid if regroup partially/completely failed. We just promote anything
		// not regrouped.
		// So, match up all that's left, plan D
		log.Debugf("topology_recovery: - RecoverDeadIntermediateMain: will next attempt to relocate up from %+v", *failedInstanceKey)

		var errs []error
		var relocatedSubordinates [](*inst.Instance)
		relocatedSubordinates, successorInstance, err, errs = inst.RelocateSubordinates(failedInstanceKey, &analysisEntry.AnalyzedInstanceMainKey, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(analysisEntry.AnalyzedInstanceMainKey)

		if len(relocatedSubordinates) > 0 {
			recoveryResolved = true
			inst.AuditOperation("recover-dead-intermediate-main", failedInstanceKey, fmt.Sprintf("Relocated subordinates under: %+v %d errors: %+v", successorInstance.Key, len(errs), errs))
		} else {
			err = log.Errorf("topology_recovery: RecoverDeadIntermediateMain failed to match up any subordinate from %+v", *failedInstanceKey)
			topologyRecovery.AddError(err)
		}
	}
	if !recoveryResolved {
		successorInstance = nil
	}
	ResolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

// checkAndRecoverDeadIntermediateMain checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadIntermediateMain(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedIntermediateMainRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		log.Debugf("topology_recovery: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMain.", analysisEntry.AnalyzedInstanceKey)
		return false, nil, err
	}

	// That's it! We must do recovery!
	recoverDeadIntermediateMainCounter.Inc(1)
	promotedSubordinate, err := RecoverDeadIntermediateMain(topologyRecovery, skipProcesses)
	if promotedSubordinate != nil {
		// success
		recoverDeadIntermediateMainSuccessCounter.Inc(1)

		if !skipProcesses {
			// Execute post intermediate-main-failover processes
			topologyRecovery.SuccessorKey = &promotedSubordinate.Key
			topologyRecovery.SuccessorAlias = promotedSubordinate.InstanceAlias
			executeProcesses(config.Config.PostIntermediateMainFailoverProcesses, "PostIntermediateMainFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadIntermediateMainFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// RecoverDeadCoMain recovers a dead co-main, complete logic inside
func RecoverDeadCoMain(topologyRecovery *TopologyRecovery, skipProcesses bool) (promotedSubordinate *inst.Instance, lostSubordinates [](*inst.Instance), err error) {
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	otherCoMainKey := &analysisEntry.AnalyzedInstanceMainKey
	otherCoMain, found, _ := inst.ReadInstance(otherCoMainKey)
	if otherCoMain == nil || !found {
		return nil, lostSubordinates, topologyRecovery.AddError(log.Errorf("RecoverDeadCoMain: could not read info for co-main %+v of %+v", *otherCoMainKey, *failedInstanceKey))
	}
	inst.AuditOperation("recover-dead-co-main", failedInstanceKey, "problem found; will recover")
	if !skipProcesses {
		if err := executeProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, lostSubordinates, topologyRecovery.AddError(err)
		}
	}

	log.Debugf("topology_recovery: RecoverDeadCoMain: will recover %+v", *failedInstanceKey)

	var coMainRecoveryType MainRecoveryType = MainRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		coMainRecoveryType = MainRecoveryGTID
	}

	log.Debugf("topology_recovery: RecoverDeadCoMain: coMainRecoveryType=%+v", coMainRecoveryType)

	var cannotReplicateSubordinates [](*inst.Instance)
	switch coMainRecoveryType {
	case MainRecoveryGTID:
		{
			lostSubordinates, _, cannotReplicateSubordinates, promotedSubordinate, err = inst.RegroupSubordinatesGTID(failedInstanceKey, true, nil)
		}
	case MainRecoveryPseudoGTID:
		{
			lostSubordinates, _, _, cannotReplicateSubordinates, promotedSubordinate, err = inst.RegroupSubordinatesPseudoGTIDIncludingSubSubordinatesOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer)
		}
	}
	topologyRecovery.AddError(err)
	lostSubordinates = append(lostSubordinates, cannotReplicateSubordinates...)

	mustPromoteOtherCoMain := config.Config.CoMainRecoveryMustPromoteOtherCoMain
	if !otherCoMain.ReadOnly {
		log.Debugf("topology_recovery: RecoverDeadCoMain: other co-main %+v is writeable hence has to be promoted", otherCoMain.Key)
		mustPromoteOtherCoMain = true
	}
	log.Debugf("topology_recovery: RecoverDeadCoMain: mustPromoteOtherCoMain? %+v", mustPromoteOtherCoMain)

	if promotedSubordinate != nil {
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedSubordinate.Key)
		if mustPromoteOtherCoMain {
			log.Debugf("topology_recovery: mustPromoteOtherCoMain. Verifying that %+v is/can be promoted", *otherCoMainKey)
			promotedSubordinate, err = replacePromotedSubordinateWithCandidate(failedInstanceKey, promotedSubordinate, otherCoMainKey)
		} else {
			// We are allowed to promote any server
			promotedSubordinate, err = replacePromotedSubordinateWithCandidate(failedInstanceKey, promotedSubordinate, nil)

			if promotedSubordinate.DataCenter == otherCoMain.DataCenter &&
				promotedSubordinate.PhysicalEnvironment == otherCoMain.PhysicalEnvironment && false {
				// and _still_ we prefer to promote the co-main! They're in same env & DC so no worries about geo issues!
				promotedSubordinate, err = replacePromotedSubordinateWithCandidate(failedInstanceKey, promotedSubordinate, otherCoMainKey)
			}
		}
		topologyRecovery.AddError(err)
	}
	if promotedSubordinate != nil {
		if mustPromoteOtherCoMain && !promotedSubordinate.Key.Equals(otherCoMainKey) {
			topologyRecovery.AddError(log.Errorf("RecoverDeadCoMain: could not manage to promote other-co-main %+v; was only able to promote %+v; CoMainRecoveryMustPromoteOtherCoMain is true, therefore failing", *otherCoMainKey, promotedSubordinate.Key))
			promotedSubordinate = nil
		}
	}
	if promotedSubordinate != nil {
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedSubordinate.Key)
	}

	// OK, we may have someone promoted. Either this was the other co-main or another subordinate.
	// Noting down that we DO NOT attempt to set a new co-main topology. We are good with remaining with a single main.
	// I tried solving the "let's promote a subordinate and create a new co-main setup" but this turns so complex due to various factors.
	// I see this as risky and not worth the questionable benefit.
	// Maybe future me is a smarter person and finds a simple solution. Unlikely. I'm getting dumber.
	//
	// ...
	// Now that we're convinved, take a look at what we can be left with:
	// Say we started with M1<->M2<-S1, with M2 failing, and we promoted S1.
	// We now have M1->S1 (because S1 is promoted), S1->M2 (because that's what it remembers), M2->M1 (because that's what it remembers)
	// !! This is an evil 3-node circle that must be broken.
	// config.Config.ApplyMySQLPromotionAfterMainFailover, if true, will cause it to break, because we would RESET SLAVE on S1
	// but we want to make sure the circle is broken no matter what.
	// So in the case we promoted not-the-other-co-main, we issue a detach-subordinate-main-host, which is a reversible operation
	if promotedSubordinate != nil && !promotedSubordinate.Key.Equals(otherCoMainKey) {
		_, err = inst.DetachSubordinateMainHost(&promotedSubordinate.Key)
		topologyRecovery.AddError(log.Errore(err))
	}

	if promotedSubordinate != nil && len(lostSubordinates) > 0 && config.Config.DetachLostSubordinatesAfterMainFailover {
		postponedFunction := func() error {
			log.Debugf("topology_recovery: - RecoverDeadCoMain: lost %+v subordinates during recovery process; detaching them", len(lostSubordinates))
			for _, subordinate := range lostSubordinates {
				subordinate := subordinate
				inst.DetachSubordinateOperation(&subordinate.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction)
	}
	if config.Config.MainFailoverLostInstancesDowntimeMinutes > 0 {
		postponedFunction := func() error {
			inst.BeginDowntime(failedInstanceKey, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, config.Config.MainFailoverLostInstancesDowntimeMinutes*60)
			for _, subordinate := range lostSubordinates {
				subordinate := subordinate
				inst.BeginDowntime(&subordinate.Key, inst.GetMaintenanceOwner(), inst.DowntimeLostInRecoveryMessage, config.Config.MainFailoverLostInstancesDowntimeMinutes*60)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction)
	}

	return promotedSubordinate, lostSubordinates, err
}

// checkAndRecoverDeadCoMain checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadCoMain(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMainRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		log.Debugf("topology_recovery: found an active or recent recovery on %+v. Will not issue another RecoverDeadCoMain.", analysisEntry.AnalyzedInstanceKey)
		return false, nil, err
	}

	// That's it! We must do recovery!
	recoverDeadCoMainCounter.Inc(1)
	promotedSubordinate, lostSubordinates, err := RecoverDeadCoMain(topologyRecovery, skipProcesses)
	ResolveRecovery(topologyRecovery, promotedSubordinate)
	if promotedSubordinate == nil {
		inst.AuditOperation("recover-dead-co-main", failedInstanceKey, "Failure: no subordinate promoted.")
	} else {
		inst.AuditOperation("recover-dead-co-main", failedInstanceKey, fmt.Sprintf("promoted: %+v", promotedSubordinate.Key))
	}
	topologyRecovery.LostSubordinates.AddInstances(lostSubordinates)
	if promotedSubordinate != nil {
		// success
		recoverDeadCoMainSuccessCounter.Inc(1)

		if config.Config.ApplyMySQLPromotionAfterMainFailover {
			log.Debugf("topology_recovery: - RecoverDeadMain: will apply MySQL changes to promoted main")
			inst.SetReadOnly(&promotedSubordinate.Key, false)
		}
		if !skipProcesses {
			// Execute post intermediate-main-failover processes
			topologyRecovery.SuccessorKey = &promotedSubordinate.Key
			topologyRecovery.SuccessorAlias = promotedSubordinate.InstanceAlias
			executeProcesses(config.Config.PostMainFailoverProcesses, "PostMainFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadCoMainFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// checkAndRecoverUnreachableMainWithStaleSubordinates executes an external process. No other action is taken.
// Returns false.
func checkAndRecoverUnreachableMainWithStaleSubordinates(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	topologyRecovery, err := AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		log.Debugf("topology_recovery: found an active or recent recovery on %+v. Will not issue another UnreachableMainWithStaleSubordinates.", analysisEntry.AnalyzedInstanceKey)
	} else {
		recoverUnreachableMainWithStaleSubordinatesCounter.Inc(1)
		if !skipProcesses {
			err := executeProcesses(config.Config.UnreachableMainWithStaleSubordinatesProcesses, "UnreachableMainWithStaleSubordinatesProcesses", topologyRecovery, false)
			if err != nil {
				recoverUnreachableMainWithStaleSubordinatesFailureCounter.Inc(1)
			} else {
				recoverUnreachableMainWithStaleSubordinatesSuccessCounter.Inc(1)
			}
		}
	}
	return false, nil, err
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverGenericProblem(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *TopologyRecovery, error) {
	return false, nil, nil
}

// Force a re-read of a topology instance; this is done because we need to substantiate a suspicion
// that we may have a failover scenario. we want to speed up reading the complete picture.
func emergentlyReadTopologyInstance(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	if existsInCacheError := emergencyReadTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted
		return
	}
	go inst.ExecuteOnTopology(func() {
		inst.ReadTopologyInstanceUnbuffered(instanceKey)
		inst.AuditOperation("emergently-read-topology-instance", instanceKey, string(analysisCode))
	})
}

// Force reading of subordinates of given instance. This is because we suspect the instance is dead, and want to speed up
// detection of replication failure from its subordinates.
func emergentlyReadTopologyInstanceSubordinates(instanceKey *inst.InstanceKey, analysisCode inst.AnalysisCode) {
	subordinates, err := inst.ReadSubordinateInstancesIncludingBinlogServerSubSubordinates(instanceKey)
	if err != nil {
		return
	}
	for _, subordinate := range subordinates {
		go emergentlyReadTopologyInstance(&subordinate.Key, analysisCode)
	}
}

// checkAndExecuteFailureDetectionProcesses tries to register for failure detection and potentially executes
// failure-detection processes.
func checkAndExecuteFailureDetectionProcesses(analysisEntry inst.ReplicationAnalysis, skipProcesses bool) (processesExecutionAttempted bool, err error) {
	if ok, _ := AttemptFailureDetectionRegistration(&analysisEntry); !ok {
		return false, nil
	}
	log.Debugf("topology_recovery: detected %+v failure on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// Execute on-detection processes
	if skipProcesses {
		return false, nil
	}
	err = executeProcesses(config.Config.OnFailureDetectionProcesses, "OnFailureDetectionProcesses", NewTopologyRecovery(analysisEntry), true)
	return true, err
}

// executeCheckAndRecoverFunction will choose the correct check & recovery function based on analysis.
// It executes the function synchronuously
func executeCheckAndRecoverFunction(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	var checkAndRecoverFunction func(analysisEntry inst.ReplicationAnalysis, candidateInstanceKey *inst.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) = nil

	switch analysisEntry.Analysis {
	case inst.DeadMain:
		checkAndRecoverFunction = checkAndRecoverDeadMain
	case inst.DeadMainAndSomeSubordinates:
		checkAndRecoverFunction = checkAndRecoverDeadMain
	case inst.DeadIntermediateMain:
		checkAndRecoverFunction = checkAndRecoverDeadIntermediateMain
	case inst.DeadIntermediateMainAndSomeSubordinates:
		checkAndRecoverFunction = checkAndRecoverDeadIntermediateMain
	case inst.DeadIntermediateMainWithSingleSubordinateFailingToConnect:
		checkAndRecoverFunction = checkAndRecoverDeadIntermediateMain
	case inst.AllIntermediateMainSubordinatesFailingToConnectOrDead:
		checkAndRecoverFunction = checkAndRecoverDeadIntermediateMain
	case inst.DeadCoMain:
		checkAndRecoverFunction = checkAndRecoverDeadCoMain
	case inst.DeadCoMainAndSomeSubordinates:
		checkAndRecoverFunction = checkAndRecoverDeadCoMain
	case inst.DeadMainAndSubordinates:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMainKey, analysisEntry.Analysis)
	case inst.UnreachableMain:
		go emergentlyReadTopologyInstanceSubordinates(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.AllMainSubordinatesNotReplicating:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case inst.FirstTierSubordinateFailingToConnectToMain:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceMainKey, analysisEntry.Analysis)
	case inst.UnreachableMainWithStaleSubordinates:
		checkAndRecoverFunction = checkAndRecoverUnreachableMainWithStaleSubordinates
	}
	// Right now this is mostly causing noise with no clear action.
	// Will revisit this in the future.
	// case inst.AllMainSubordinatesStale:
	// 	checkAndRecoverFunction = checkAndRecoverGenericProblem

	if checkAndRecoverFunction == nil {
		// Unhandled problem type
		return false, nil, nil
	}
	// we have a recovery function; its execution still depends on filters if not disabled.
	log.Debugf("executeCheckAndRecoverFunction: proceeeding with %+v; skipProcesses: %+v", analysisEntry.AnalyzedInstanceKey, skipProcesses)

	if _, err := checkAndExecuteFailureDetectionProcesses(analysisEntry, skipProcesses); err != nil {
		return false, nil, err
	}

	recoveryAttempted, topologyRecovery, err = checkAndRecoverFunction(analysisEntry, candidateInstanceKey, forceInstanceRecovery, skipProcesses)
	if !recoveryAttempted {
		return recoveryAttempted, topologyRecovery, err
	}
	if topologyRecovery == nil {
		return recoveryAttempted, topologyRecovery, err
	}
	if !skipProcesses {
		if topologyRecovery.SuccessorKey == nil {
			// Execute general unsuccessful post failover processes
			executeProcesses(config.Config.PostUnsuccessfulFailoverProcesses, "PostUnsuccessfulFailoverProcesses", topologyRecovery, false)
		} else {
			// Execute general post failover processes
			inst.EndDowntime(topologyRecovery.SuccessorKey)
			executeProcesses(config.Config.PostFailoverProcesses, "PostFailoverProcesses", topologyRecovery, false)
		}
	}
	topologyRecovery.InvokePostponed()
	return recoveryAttempted, topologyRecovery, err
}

// CheckAndRecover is the main entry point for the recovery mechanism
func CheckAndRecover(specificInstance *inst.InstanceKey, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedSubordinateKey *inst.InstanceKey, err error) {
	// Allow the analysis to run evern if we don't want to recover
	replicationAnalysis, err := inst.GetReplicationAnalysis("", true, true)
	if err != nil {
		return false, nil, log.Errore(err)
	}
	// Check for recovery being disabled globally
	recoveryDisabledGlobally, err := IsRecoveryDisabled()
	if err != nil {
		log.Warningf("Unable to determine if recovery is disabled globally: %v", err)
	}
	if *config.RuntimeCLIFlags.Noop {
		log.Debugf("--noop provided; will not execute processes")
		skipProcesses = true
	}
	for _, analysisEntry := range replicationAnalysis {
		if specificInstance != nil {
			// We are looking for a specific instance; if this is not the one, skip!
			if !specificInstance.Equals(&analysisEntry.AnalyzedInstanceKey) {
				continue
			}
		}
		if analysisEntry.IsDowntimed && specificInstance == nil {
			// Only recover a downtimed server if explicitly requested
			continue
		}

		if specificInstance != nil {
			// force mode. Keep it synchronuous
			var topologyRecovery *TopologyRecovery
			recoveryAttempted, topologyRecovery, err = executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
			if topologyRecovery != nil {
				promotedSubordinateKey = topologyRecovery.SuccessorKey
			}
		} else if recoveryDisabledGlobally {
			log.Infof("CheckAndRecover: InstanceKey: %+v, candidateInstanceKey: %+v, "+
				"skipProcesses: %v: NOT Recovering host (disabled globally)",
				analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)
		} else {
			go executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, false, skipProcesses)
		}
	}
	return recoveryAttempted, promotedSubordinateKey, err
}

// ForceExecuteRecovery can be called to issue a recovery process even if analysis says there is no recovery case.
// The caller of this function injects the type of analysis it wishes the function to assume.
// By calling this function one takes responsibility for one's actions.
func ForceExecuteRecovery(clusterName string, analysisCode inst.AnalysisCode, failedInstanceKey *inst.InstanceKey, candidateInstanceKey *inst.InstanceKey, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error) {
	clusterInfo, err := inst.ReadClusterInfo(clusterName)
	if err != nil {
		return recoveryAttempted, topologyRecovery, err
	}

	analysisEntry := inst.ReplicationAnalysis{
		Analysis:            analysisCode,
		ClusterDetails:      *clusterInfo,
		AnalyzedInstanceKey: *failedInstanceKey,
	}
	return executeCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses)
}

// ForceMainTakeover *trusts* main of given cluster is dead and fails over to designated instance,
// which has to be its direct child.
func ForceMainTakeover(clusterName string, destination *inst.Instance) (topologyRecovery *TopologyRecovery, err error) {
	clusterMains, err := inst.ReadClusterWriteableMain(clusterName)
	if err != nil {
		return nil, fmt.Errorf("Cannot deduce cluster main for %+v", clusterName)
	}
	if len(clusterMains) != 1 {
		return nil, fmt.Errorf("Cannot deduce cluster main for %+v", clusterName)
	}
	clusterMain := clusterMains[0]

	if !destination.MainKey.Equals(&clusterMain.Key) {
		return nil, fmt.Errorf("You may only promote a direct child of the main %+v. The main of %+v is %+v.", clusterMain.Key, destination.Key, destination.MainKey)
	}
	log.Debugf("Will demote %+v and promote %+v instead", clusterMain.Key, destination.Key)

	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(clusterName, inst.DeadMain, &clusterMain.Key, &destination.Key, false)
	if err != nil {
		return nil, err
	}
	if !recoveryAttempted {
		return nil, fmt.Errorf("Unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, fmt.Errorf("Recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		return nil, fmt.Errorf("Recovery attempted yet no subordinate promoted")
	}
	return topologyRecovery, nil
}

// GracefulMainTakeover will demote main of existing topology and promote its
// direct replica instead.
// It expects that replica to have no siblings.
// This function is graceful in that it will first lock down the main, then wait
// for the designated replica to catch up with last position.
func GracefulMainTakeover(clusterName string) (topologyRecovery *TopologyRecovery, promotedMainCoordinates *inst.BinlogCoordinates, err error) {
	clusterMains, err := inst.ReadClusterWriteableMain(clusterName)
	if err != nil {
		return nil, nil, fmt.Errorf("Cannot deduce cluster main for %+v", clusterName)
	}
	if len(clusterMains) != 1 {
		return nil, nil, fmt.Errorf("Cannot deduce cluster main for %+v. Found %+v potential mains", clusterName, len(clusterMains))
	}
	clusterMain := clusterMains[0]
	if len(clusterMain.SubordinateHosts) == 0 {
		return nil, nil, fmt.Errorf("Main %+v doesn't seem to have replicas", clusterMain.Key)
	}
	if len(clusterMain.SubordinateHosts) > 1 {
		return nil, nil, fmt.Errorf("GracefulMainTakeover: main %+v should only have one replica (making the takeover safe and simple), but has %+v. Aborting", clusterMain.Key, len(clusterMain.SubordinateHosts))
	}

	designatedInstanceKey := &(clusterMain.SubordinateHosts.GetInstanceKeys()[0])
	designatedInstance, err := inst.ReadTopologyInstanceUnbuffered(designatedInstanceKey)
	if err != nil {
		return nil, nil, err
	}
	mainOfDesigntaedInstance, err := inst.GetInstanceMain(designatedInstance)
	if err != nil {
		return nil, nil, err
	}
	if !mainOfDesigntaedInstance.Key.Equals(&clusterMain.Key) {
		return nil, nil, fmt.Errorf("Sanity check failure. It seems like the designated instance %+v does not replicate from the main %+v (designated instance's main key is %+v). This error is strange. Panicking", designatedInstance.Key, clusterMain.Key, designatedInstance.MainKey)
	}
	if !designatedInstance.HasReasonableMaintenanceReplicationLag() {
		return nil, nil, fmt.Errorf("Desginated instance %+v seems to be lagging to much for thie operation. Aborting.", designatedInstance.Key)
	}
	log.Debugf("Will demote %+v and promote %+v instead", clusterMain.Key, designatedInstance.Key)

	if designatedInstance, err = inst.StopSubordinate(&designatedInstance.Key); err != nil {
		return nil, nil, err
	}
	log.Debugf("Will set %+v as read_only", clusterMain.Key)
	if clusterMain, err = inst.SetReadOnly(&clusterMain.Key, true); err != nil {
		return nil, nil, err
	}

	log.Debugf("Will advance %+v to main coordinates %+v", designatedInstance.Key, clusterMain.SelfBinlogCoordinates)
	if designatedInstance, err = inst.StartSubordinateUntilMainCoordinates(&designatedInstance.Key, &clusterMain.SelfBinlogCoordinates); err != nil {
		return nil, nil, err
	}
	promotedMainCoordinates = &designatedInstance.SelfBinlogCoordinates

	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(clusterName, inst.DeadMain, &clusterMain.Key, &designatedInstance.Key, false)
	if err != nil {
		return nil, nil, err
	}
	if !recoveryAttempted {
		return nil, nil, fmt.Errorf("Unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, nil, fmt.Errorf("Recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		return nil, nil, fmt.Errorf("Recovery attempted yet no subordinate promoted")
	}
	return topologyRecovery, promotedMainCoordinates, nil
}
