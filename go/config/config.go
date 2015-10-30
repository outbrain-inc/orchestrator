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

package config

import (
	"encoding/json"
	"os"

	"code.google.com/p/gcfg"

	"github.com/outbrain/golib/log"
)

// Configuration makes for orchestrator configuration input, which can be provided by user via JSON formatted file.
// Some of the parameteres have reasonable default values, and some (like database credentials) are
// strictly expected from user.
type Configuration struct {
	Debug                                      bool // set debug mode (similar to --debug option)
	EnableSyslog                               bool // Should logs be directed (in addition) to syslog daemon?
	ListenAddress                              string
	AgentsServerPort                           string // port orchestrator agents talk back to
	MySQLTopologyUser                          string
	MySQLTopologyPassword                      string // my.cnf style configuration file from where to pick credentials. Expecting `user`, `password` under `[client]` section
	MySQLTopologyCredentialsConfigFile         string
	MySQLTopologySSLPrivateKeyFile             string // Private key file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLCertFile                   string // Certificate PEM file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLCAFile                     string // Certificate Authority PEM file used to authenticate with a Topology mysql instance with TLS
	MySQLTopologySSLSkipVerify                 bool   // If true, do not strictly validate mutual TLS certs for Topology mysql instances
	MySQLTopologyUseMutualTLS                  bool   // Turn on TLS authentication with the Topology MySQL instances
	MySQLTopologyMaxPoolConnections            int    // Max concurrent connections on any topology instance
	DatabaselessMode__experimental             bool   // !!!EXPERIMENTAL!!! Orchestrator will execute without speaking to a backend database; super-standalone mode
	MySQLOrchestratorHost                      string
	MySQLOrchestratorPort                      uint
	MySQLOrchestratorDatabase                  string
	MySQLOrchestratorUser                      string
	MySQLOrchestratorPassword                  string
	MySQLOrchestratorCredentialsConfigFile     string   // my.cnf style configuration file from where to pick credentials. Expecting `user`, `password` under `[client]` section
	MySQLOrchestratorSSLPrivateKeyFile         string   // Private key file used to authenticate with the Orchestrator mysql instance with TLS
	MySQLOrchestratorSSLCertFile               string   // Certificate PEM file used to authenticate with the Orchestrator mysql instance with TLS
	MySQLOrchestratorSSLCAFile                 string   // Certificate Authority PEM file used to authenticate with the Orchestrator mysql instance with TLS
	MySQLOrchestratorSSLSkipVerify             bool     // If true, do not strictly validate mutual TLS certs for the Orchestrator mysql instances
	MySQLOrchestratorUseMutualTLS              bool     // Turn on TLS authentication with the Orchestrator MySQL instance
	MySQLConnectTimeoutSeconds                 int      // Number of seconds before connection is aborted (driver-side)
	DefaultInstancePort                        int      // In case port was not specified on command line
	SkipOrchestratorDatabaseUpdate             bool     // When false, orchestrator will attempt to create & update all tables in backend database; when true, this is skipped. It makes sense to skip on command-line invocations and to enable for http or occasional invocations, or just after upgrades
	SmartOrchestratorDatabaseUpdate            bool     // When true, orchestrator deploys internal schema based on deployment history, and only applies changes known to be uncommitted
	SlaveLagQuery                              string   // custom query to check on slave lg (e.g. heartbeat table)
	SlaveStartPostWaitMilliseconds             int      // Time to wait after START SLAVE before re-readong instance (give slave chance to connect to master)
	DiscoverByShowSlaveHosts                   bool     // Attempt SHOW SLAVE HOSTS before PROCESSLIST
	InstancePollSeconds                        uint     // Number of seconds between instance reads
	ReadLongRunningQueries                     bool     // Whether orchestrator should read and record current long running executing queries.
	UnseenInstanceForgetHours                  uint     // Number of hours after which an unseen instance is forgotten
	SnapshotTopologiesIntervalHours            uint     // Interval in hour between snapshot-topologies invocation. Default: 0 (disabled)
	DiscoveryPollSeconds                       uint     // Auto/continuous discovery of instances sleep time between polls
	InstanceBulkOperationsWaitTimeoutSeconds   uint     // Time to wait on a single instance when doing bulk (many instances) operation
	ActiveNodeExpireSeconds                    uint     // Maximum time to wait for active node to send keepalive before attempting to take over as active node.
	HostnameResolveMethod                      string   // Method by which to "normalize" hostname ("none"/"default"/"cname")
	MySQLHostnameResolveMethod                 string   // Method by which to "normalize" hostname via MySQL server. ("none"/"@@hostname"/"@@report_host"; default "@@hostname")
	SkipBinlogServerUnresolveCheck             bool     // Skip the double-check that an unresolved hostname resolves back to same hostname for binlog servers
	ExpiryHostnameResolvesMinutes              int      // Number of minutes after which to expire hostname-resolves
	RejectHostnameResolvePattern               string   // Regexp pattern for resolved hostname that will not be accepted (not cached, not written to db). This is done to avoid storing wrong resolves due to network glitches.
	ReasonableReplicationLagSeconds            int      // Above this value is considered a problem
	ProblemIgnoreHostnameFilters               []string // Will minimize problem visualization for hostnames matching given regexp filters
	VerifyReplicationFilters                   bool     // Include replication filters check before approving topology refactoring
	MaintenanceOwner                           string   // (Default) name of maintenance owner to use if none provided
	ReasonableMaintenanceReplicationLagSeconds int      // Above this value move-up and move-below are blocked
	MaintenanceExpireMinutes                   uint     // Minutes after which a maintenance flag is considered stale and is cleared
	MaintenancePurgeDays                       uint     // Days after which maintenance entries are purged from the database
	CandidateInstanceExpireMinutes             uint     // Minutes after which a suggestion to use an instance as a candidate slave (to be preferably promoted on master failover) is expired.
	AuditLogFile                               string   // Name of log file for audit operations. Disabled when empty.
	AuditToSyslog                              bool     // If true, audit messages are written to syslog
	AuditPageSize                              int
	AuditPurgeDays                             uint   // Days after which audit entries are purged from the database
	RemoveTextFromHostnameDisplay              string // Text to strip off the hostname on cluster/clusters pages
	ReadOnly                                   bool
	AuthenticationMethod                       string            // Type of autherntication to use, if any. "" for none, "basic" for BasicAuth, "multi" for advanced BasicAuth, "proxy" for forwarded credentials via reverse proxy
	HTTPAuthUser                               string            // Username for HTTP Basic authentication (blank disables authentication)
	HTTPAuthPassword                           string            // Password for HTTP Basic authentication
	AuthUserHeader                             string            // HTTP header indicating auth user, when AuthenticationMethod is "proxy"
	PowerAuthUsers                             []string          // On AuthenticationMethod == "proxy", list of users that can make changes. All others are read-only.
	ClusterNameToAlias                         map[string]string // map between regex matching cluster name to a human friendly alias
	DetectClusterAliasQuery                    string            // Optional query (executed on topology instance) that returns the alias of a cluster. Query will only be executed on cluster master (though until the topology's master is resovled it may execute on other/all slaves). If provided, must return one row, one column
	DetectClusterDomainQuery                   string            // Optional query (executed on topology instance) that returns the VIP/CNAME/Alias/whatever domain name for the master of this cluster. Query will only be executed on cluster master (though until the topology's master is resovled it may execute on other/all slaves). If provided, must return one row, one column
	DataCenterPattern                          string            // Regexp pattern with one group, extracting the datacenter name from the hostname
	PhysicalEnvironmentPattern                 string            // Regexp pattern with one group, extracting physical environment info from hostname (e.g. combination of datacenter & prod/dev env)
	PromotionIgnoreHostnameFilters             []string          // Orchestrator will not promote slaves with hostname matching pattern (via -c recovery; for example, avoid promoting dev-dedicated machines)
	ServeAgentsHttp                            bool              // Spawn another HTTP interface dedicated for orcehstrator-agent
	AgentsUseSSL                               bool              // When "true" orchestrator will listen on agents port with SSL as well as connect to agents via SSL
	AgentsUseMutualTLS                         bool              // When "true" Use mutual TLS for the server to agent communication
	AgentSSLSkipVerify                         bool              // When using SSL for the Agent, should we ignore SSL certification error
	AgentSSLPrivateKeyFile                     string            // Name of Agent SSL private key file, applies only when AgentsUseSSL = true
	AgentSSLCertFile                           string            // Name of Agent SSL certification file, applies only when AgentsUseSSL = true
	AgentSSLCAFile                             string            // Name of the Agent Certificate Authority file, applies only when AgentsUseSSL = true
	AgentSSLValidOUs                           []string          // Valid organizational units when using mutual TLS to communicate with the agents
	UseSSL                                     bool              // Use SSL on the server web port
	UseMutualTLS                               bool              // When "true" Use mutual TLS for the server's web and API connections
	SSLSkipVerify                              bool              // When using SSL, should we ignore SSL certification error
	SSLPrivateKeyFile                          string            // Name of SSL private key file, applies only when UseSSL = true
	SSLCertFile                                string            // Name of SSL certification file, applies only when UseSSL = true
	SSLCAFile                                  string            // Name of the Certificate Authority file, applies only when UseSSL = true
	SSLValidOUs                                []string          // Valid organizational units when using mutual TLS
	StatusEndpoint                             string            // Override the status endpoint.  Defaults to '/api/status'
	StatusSimpleHealth                         bool              // If true, calling the status endpoint will use the simplified health check
	StatusOUVerify                             bool              // If true, try to verify OUs when Mutual TLS is on.  Defaults to false
	HttpTimeoutSeconds                         int               // Number of idle seconds before HTTP GET request times out (when accessing orchestrator-agent)
	AgentPollMinutes                           uint              // Minutes between agent polling
	AgentAutoDiscover                          bool              // If true, instances should automatically discover when an agent is submitted
	UnseenAgentForgetHours                     uint              // Number of hours after which an unseen agent is forgotten
	StaleSeedFailMinutes                       uint              // Number of minutes after which a stale (no progress) seed is considered failed.
	SeedAcceptableBytesDiff                    int64             // Difference in bytes between seed source & target data size that is still considered as successful copy
	PseudoGTIDPattern                          string            // Pattern to look for in binary logs that makes for a unique entry (pseudo GTID). When empty, Pseudo-GTID based refactoring is disabled.
	PseudoGTIDMonotonicHint                    string            // subtring in Pseudo-GTID entry which indicates Pseudo-GTID entries are expected to be monotonically increasing
	DetectPseudoGTIDQuery                      string            // Optional query which is used to authoritatively decide whether pseudo gtid is enabled on instance
	AllowGTIDMode                              bool              // If true, GTID mode can be enabled by powerusers via the Web UI or API.  Otherwise GTID mode can not be enabled.
	BinlogEventsChunkSize                      int               // Chunk size (X) for SHOW BINLOG|RELAYLOG EVENTS LIMIT ?,X statements. Smaller means less locking and mroe work to be done
	BufferBinlogEvents                         bool              // Should we used buffered read on SHOW BINLOG|RELAYLOG EVENTS -- releases the database lock sooner (recommended)
	SkipBinlogEventsContaining                 []string          // When scanning/comparing binlogs for Pseudo-GTID, skip entries containing given texts. These are NOT regular expressions (would consume too much CPU while scanning binlogs), just substrings to find.
	FailureDetectionPeriodBlockMinutes         int               // The time for which an instance's failure discovery is kept "active", so as to avoid concurrent "discoveries" of the instance's failure; this preceeds any recovery process, if any.
	RecoveryPollSeconds                        int               // Interval between checks for a recovery scenario and initiation of a recovery process
	RecoveryPeriodBlockMinutes                 int               // The time for which an instance's recovery is kept "active", so as to avoid concurrent recoveries on smae instance as well as flapping
	RecoveryIgnoreHostnameFilters              []string          // Recovery analysis will completely ignore hosts matching given patterns
	RecoverMasterClusterFilters                []string          // Only do master recovery on clusters matching these regexp patterns (of course the ".*" pattern matches everything)
	RecoverIntermediateMasterClusterFilters    []string          // Only do IM recovery on clusters matching these regexp patterns (of course the ".*" pattern matches everything)
	OnFailureDetectionProcesses                []string          // Processes to execute when detecting a failover scenario (before making a decision whether to failover or not). May and should use some of these placeholders: {failureType}, {failureDescription}, {failedHost}, {failureCluster}, {failureClusterAlias}, {failedPort}, {successorHost}, {successorPort}, {countSlaves}, {slaveHosts}, {isDowntimed}, {autoMasterRecovery}, {autoIntermediateMasterRecovery}
	PreFailoverProcesses                       []string          // Processes to execute before doing a failover (aborting operation should any once of them exits with non-zero code; order of execution undefined). May and should use some of these placeholders: {failureType}, {failureDescription}, {failedHost}, {failureCluster}, {failureClusterAlias}, {failedPort}, {successorHost}, {successorPort}, {countSlaves}, {slaveHosts}, {isDowntimed}
	PostFailoverProcesses                      []string          // Processes to execute after doing a failover (order of execution undefined). May and should use some of these placeholders: {failureType}, {failureDescription}, {failedHost}, {failureCluster}, {failureClusterAlias}, {failedPort}, {successorHost}, {successorPort}, {countSlaves}, {slaveHosts}, {isDowntimed}, {isSuccessful}, {lostSlaves}
	PostUnsuccessfulFailoverProcesses          []string          // Processes to execute after a not-completely-successful failover (order of execution undefined). May and should use some of these placeholders: {failureType}, {failureDescription}, {failedHost}, {failureCluster}, {failureClusterAlias}, {failedPort}, {successorHost}, {successorPort}, {countSlaves}, {slaveHosts}, {isDowntimed}, {isSuccessful}, {lostSlaves}
	PostMasterFailoverProcesses                []string          // Processes to execute after doing a master failover (order of execution undefined). Uses same placeholders as PostFailoverProcesses
	PostIntermediateMasterFailoverProcesses    []string          // Processes to execute after doing a master failover (order of execution undefined). Uses same placeholders as PostFailoverProcesses
	DetachLostSlavesAfterMasterFailover        bool              // Should slaves that are not to be lost in master recovery (i.e. were more up-to-date than promoted slave) be forcibly detached
	MasterFailoverLostInstancesDowntimeMinutes uint              // Number of minutes to downtime any server that was lost after a master failover (including failed master & lost slaves). 0 to disable
	PostponeSlaveRecoveryOnLagMinutes          uint              // On crash recovery, slaves that are lagging more than given minutes are only resurrected late in the recovery process, after master/IM has been elected and processes executed. Value of 0 disables this feature
	OSCIgnoreHostnameFilters                   []string          // OSC slaves recommendation will ignore slave hostnames matching given patterns
	GraphiteAddr                               string            // Optional; address of graphite port. If supplied, metrics will be written here
	GraphitePath                               string            // Prefix for graphite path. May include {hostname} magic placeholder
	GraphiteConvertHostnameDotsToUnderscores   bool              // If true, then hostname's dots are converted to underscores before being used in graphite path
}

var Config *Configuration = NewConfiguration()
var readFileNames []string

func NewConfiguration() *Configuration {
	return &Configuration{
		Debug:                                      false,
		EnableSyslog:                               false,
		ListenAddress:                              ":3000",
		AgentsServerPort:                           ":3001",
		StatusEndpoint:                             "/api/status",
		StatusSimpleHealth:                         true,
		StatusOUVerify:                             false,
		MySQLOrchestratorPort:                      3306,
		MySQLTopologyMaxPoolConnections:            3,
		MySQLTopologyUseMutualTLS:                  false,
		DatabaselessMode__experimental:             false,
		MySQLOrchestratorUseMutualTLS:              false,
		MySQLConnectTimeoutSeconds:                 5,
		DefaultInstancePort:                        3306,
		SkipOrchestratorDatabaseUpdate:             false,
		SmartOrchestratorDatabaseUpdate:            false,
		InstancePollSeconds:                        60,
		ReadLongRunningQueries:                     true,
		UnseenInstanceForgetHours:                  240,
		SnapshotTopologiesIntervalHours:            0,
		SlaveStartPostWaitMilliseconds:             1000,
		DiscoverByShowSlaveHosts:                   false,
		DiscoveryPollSeconds:                       5,
		InstanceBulkOperationsWaitTimeoutSeconds:   10,
		ActiveNodeExpireSeconds:                    60,
		HostnameResolveMethod:                      "cname",
		MySQLHostnameResolveMethod:                 "@@hostname",
		SkipBinlogServerUnresolveCheck:             true,
		ExpiryHostnameResolvesMinutes:              60,
		RejectHostnameResolvePattern:               "",
		ReasonableReplicationLagSeconds:            10,
		ProblemIgnoreHostnameFilters:               []string{},
		VerifyReplicationFilters:                   false,
		MaintenanceOwner:                           "orchestrator",
		ReasonableMaintenanceReplicationLagSeconds: 20,
		MaintenanceExpireMinutes:                   10,
		MaintenancePurgeDays:                       365,
		CandidateInstanceExpireMinutes:             60,
		AuditLogFile:                               "",
		AuditToSyslog:                              false,
		AuditPageSize:                              20,
		AuditPurgeDays:                             365,
		RemoveTextFromHostnameDisplay:              "",
		ReadOnly:                                   false,
		AuthenticationMethod:                       "basic",
		HTTPAuthUser:                               "",
		HTTPAuthPassword:                           "",
		AuthUserHeader:                             "X-Forwarded-User",
		PowerAuthUsers:                             []string{"*"},
		ClusterNameToAlias:                         make(map[string]string),
		DetectClusterAliasQuery:                    "",
		DetectClusterDomainQuery:                   "",
		DataCenterPattern:                          "",
		PhysicalEnvironmentPattern:                 "",
		PromotionIgnoreHostnameFilters:             []string{},
		ServeAgentsHttp:                            false,
		AgentsUseSSL:                               false,
		AgentsUseMutualTLS:                         false,
		AgentSSLValidOUs:                           []string{},
		AgentSSLSkipVerify:                         false,
		AgentSSLPrivateKeyFile:                     "",
		AgentSSLCertFile:                           "",
		AgentSSLCAFile:                             "",
		UseSSL:                                     false,
		UseMutualTLS:                               false,
		SSLValidOUs:                                []string{},
		SSLSkipVerify:                              false,
		SSLPrivateKeyFile:                          "",
		SSLCertFile:                                "",
		SSLCAFile:                                  "",
		HttpTimeoutSeconds:                         60,
		AgentPollMinutes:                           60,
		AgentAutoDiscover:                          false,
		UnseenAgentForgetHours:                     6,
		StaleSeedFailMinutes:                       60,
		SeedAcceptableBytesDiff:                    8192,
		PseudoGTIDPattern:                          "",
		PseudoGTIDMonotonicHint:                    "",
		DetectPseudoGTIDQuery:                      "",
		AllowGTIDMode:                              true,
		BinlogEventsChunkSize:                      10000,
		BufferBinlogEvents:                         true,
		SkipBinlogEventsContaining:                 []string{},
		FailureDetectionPeriodBlockMinutes:         60,
		RecoveryPollSeconds:                        10,
		RecoveryPeriodBlockMinutes:                 60,
		RecoveryIgnoreHostnameFilters:              []string{},
		RecoverMasterClusterFilters:                []string{},
		RecoverIntermediateMasterClusterFilters:    []string{},
		OnFailureDetectionProcesses:                []string{},
		PreFailoverProcesses:                       []string{},
		PostMasterFailoverProcesses:                []string{},
		PostIntermediateMasterFailoverProcesses:    []string{},
		PostFailoverProcesses:                      []string{},
		PostUnsuccessfulFailoverProcesses:          []string{},
		DetachLostSlavesAfterMasterFailover:        true,
		MasterFailoverLostInstancesDowntimeMinutes: 0,
		PostponeSlaveRecoveryOnLagMinutes:          0,
		OSCIgnoreHostnameFilters:                   []string{},
		GraphiteAddr:                               "",
		GraphitePath:                               "",
		GraphiteConvertHostnameDotsToUnderscores:   true,
	}
}

// read reads configuration from given file, or silently skips if the file does not exist.
// If the file does exist, then it is expected to be in valid JSON format or the function bails out.
func read(file_name string) (*Configuration, error) {
	file, err := os.Open(file_name)
	if err == nil {
		decoder := json.NewDecoder(file)
		err := decoder.Decode(Config)
		if err == nil {
			log.Infof("Read config: %s", file_name)
		} else {
			log.Fatal("Cannot read config file:", file_name, err)
		}
		if Config.MySQLOrchestratorCredentialsConfigFile != "" {
			mySQLConfig := struct {
				Client struct {
					User     string
					Password string
				}
			}{}
			err := gcfg.ReadFileInto(&mySQLConfig, Config.MySQLOrchestratorCredentialsConfigFile)
			if err != nil {
				log.Fatalf("Failed to parse gcfg data from file: %+v", err)
			} else {
				log.Debugf("Parsed orchestrator credentials from %s", Config.MySQLOrchestratorCredentialsConfigFile)
				Config.MySQLOrchestratorUser = mySQLConfig.Client.User
				Config.MySQLOrchestratorPassword = mySQLConfig.Client.Password
			}
		}
		if Config.MySQLTopologyCredentialsConfigFile != "" {
			mySQLConfig := struct {
				Client struct {
					User     string
					Password string
				}
			}{}
			err := gcfg.ReadFileInto(&mySQLConfig, Config.MySQLTopologyCredentialsConfigFile)
			if err != nil {
				log.Fatalf("Failed to parse gcfg data from file: %+v", err)
			} else {
				log.Debugf("Parsed topology credentials from %s", Config.MySQLTopologyCredentialsConfigFile)
				Config.MySQLTopologyUser = mySQLConfig.Client.User
				Config.MySQLTopologyPassword = mySQLConfig.Client.Password
			}
		}

	}
	return Config, err
}

// Read reads configuration from zero, either, some or all given files, in order of input.
// A file can override configuration provided in previous file.
func Read(file_names ...string) *Configuration {
	for _, file_name := range file_names {
		read(file_name)
	}
	readFileNames = file_names
	return Config
}

// ForceRead reads configuration from given file name or bails out if it fails
func ForceRead(file_name string) *Configuration {
	_, err := read(file_name)
	if err != nil {
		log.Fatal("Cannot read config file:", file_name, err)
	}
	readFileNames = []string{file_name}
	return Config
}

func Reload() *Configuration {
	for _, file_name := range readFileNames {
		read(file_name)
	}
	return Config
}
