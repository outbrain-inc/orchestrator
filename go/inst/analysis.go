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
	"strings"
)

type AnalysisCode string
type StructureAnalysisCode string

const (
	NoProblem                                             AnalysisCode = "NoProblem"
	DeadMainWithoutSubordinates                                            = "DeadMainWithoutSubordinates"
	DeadMain                                                         = "DeadMain"
	DeadMainAndSubordinates                                                = "DeadMainAndSubordinates"
	DeadMainAndSomeSubordinates                                            = "DeadMainAndSomeSubordinates"
	UnreachableMainWithStaleSubordinates                                   = "UnreachableMainWithStaleSubordinates"
	UnreachableMain                                                  = "UnreachableMain"
	MainSingleSubordinateNotReplicating                                    = "MainSingleSubordinateNotReplicating"
	MainSingleSubordinateDead                                              = "MainSingleSubordinateDead"
	AllMainSubordinatesNotReplicating                                      = "AllMainSubordinatesNotReplicating"
	AllMainSubordinatesNotReplicatingOrDead                                = "AllMainSubordinatesNotReplicatingOrDead"
	AllMainSubordinatesStale                                               = "AllMainSubordinatesStale"
	MainWithoutSubordinates                                                = "MainWithoutSubordinates"
	DeadCoMain                                                       = "DeadCoMain"
	DeadCoMainAndSomeSubordinates                                          = "DeadCoMainAndSomeSubordinates"
	UnreachableCoMain                                                = "UnreachableCoMain"
	AllCoMainSubordinatesNotReplicating                                    = "AllCoMainSubordinatesNotReplicating"
	DeadIntermediateMain                                             = "DeadIntermediateMain"
	DeadIntermediateMainWithSingleSubordinate                              = "DeadIntermediateMainWithSingleSubordinate"
	DeadIntermediateMainWithSingleSubordinateFailingToConnect              = "DeadIntermediateMainWithSingleSubordinateFailingToConnect"
	DeadIntermediateMainAndSomeSubordinates                                = "DeadIntermediateMainAndSomeSubordinates"
	UnreachableIntermediateMain                                      = "UnreachableIntermediateMain"
	AllIntermediateMainSubordinatesFailingToConnectOrDead                  = "AllIntermediateMainSubordinatesFailingToConnectOrDead"
	AllIntermediateMainSubordinatesNotReplicating                          = "AllIntermediateMainSubordinatesNotReplicating"
	FirstTierSubordinateFailingToConnectToMain                             = "FirstTierSubordinateFailingToConnectToMain"
	BinlogServerFailingToConnectToMain                               = "BinlogServerFailingToConnectToMain"
)

const (
	StatementAndMixedLoggingSubordinatesStructureWarning StructureAnalysisCode = "StatementAndMixedLoggingSubordinatesStructureWarning"
	StatementAndRowLoggingSubordinatesStructureWarning                         = "StatementAndRowLoggingSubordinatesStructureWarning"
	MixedAndRowLoggingSubordinatesStructureWarning                             = "MixedAndRowLoggingSubordinatesStructureWarning"
	MultipleMajorVersionsLoggingSubordinates                                   = "MultipleMajorVersionsLoggingSubordinates"
)

// ReplicationAnalysis notes analysis on replication chain status, per instance
type ReplicationAnalysis struct {
	AnalyzedInstanceKey                     InstanceKey
	AnalyzedInstanceMainKey               InstanceKey
	ClusterDetails                          ClusterInfo
	IsMain                                bool
	IsCoMain                              bool
	LastCheckValid                          bool
	CountSubordinates                             uint
	CountValidSubordinates                        uint
	CountValidReplicatingSubordinates             uint
	CountSubordinatesFailingToConnectToMain     uint
	CountStaleSubordinates                        uint
	ReplicationDepth                        uint
	SubordinateHosts                              InstanceKeyMap
	IsFailingToConnectToMain              bool
	Analysis                                AnalysisCode
	Description                             string
	StructureAnalysis                       []StructureAnalysisCode
	IsDowntimed                             bool
	DowntimeEndTimestamp                    string
	DowntimeRemainingSeconds                int
	IsBinlogServer                          bool
	PseudoGTIDImmediateTopology             bool
	OracleGTIDImmediateTopology             bool
	MariaDBGTIDImmediateTopology            bool
	BinlogServerImmediateTopology           bool
	CountStatementBasedLoggingSubordinates        uint
	CountMixedBasedLoggingSubordinates            uint
	CountRowBasedLoggingSubordinates              uint
	CountDistinctMajorVersionsLoggingSubordinates uint
}

type ReplicationAnalysisChangelog struct {
	AnalyzedInstanceKey InstanceKey
	Changelog           string
}

// ReadSubordinateHostsFromString parses and reads subordinate keys from comma delimited string
func (this *ReplicationAnalysis) ReadSubordinateHostsFromString(subordinateHostsString string) error {
	this.SubordinateHosts = *NewInstanceKeyMap()
	return this.SubordinateHosts.ReadCommaDelimitedList(subordinateHostsString)
}

// AnalysisString returns a human friendly description of all analysis issues
func (this *ReplicationAnalysis) AnalysisString() string {
	result := []string{}
	if this.Analysis != NoProblem {
		result = append(result, string(this.Analysis))
	}
	for _, structureAnalysis := range this.StructureAnalysis {
		result = append(result, string(structureAnalysis))
	}
	return strings.Join(result, ", ")
}
