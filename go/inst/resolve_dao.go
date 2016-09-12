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
	"github.com/outbrain/golib/log"
	"github.com/outbrain/golib/sqlutils"
	"github.com/outbrain/orchestrator/go/config"
	"github.com/outbrain/orchestrator/go/db"
	"github.com/rcrowley/go-metrics"
)

var writeResolvedHostnameCounter = metrics.NewCounter()
var writeUnresolvedHostnameCounter = metrics.NewCounter()
var readResolvedHostnameCounter = metrics.NewCounter()
var readUnresolvedHostnameCounter = metrics.NewCounter()
var readAllResolvedHostnamesCounter = metrics.NewCounter()

func init() {
	metrics.Register("resolve.write_resolved", writeResolvedHostnameCounter)
	metrics.Register("resolve.write_unresolved", writeUnresolvedHostnameCounter)
	metrics.Register("resolve.read_resolved", readResolvedHostnameCounter)
	metrics.Register("resolve.read_unresolved", readUnresolvedHostnameCounter)
	metrics.Register("resolve.read_resolved_all", readAllResolvedHostnamesCounter)
}

// WriteResolvedHostname stores a hostname and the resolved hostname to backend database
func WriteResolvedHostname(hostname string, resolvedHostname string) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
			insert into  
					hostname_resolve (hostname, resolved_hostname, resolved_timestamp)
				values
					(?, ?, NOW())
				on duplicate key update
					resolved_hostname = VALUES(resolved_hostname), 
					resolved_timestamp = VALUES(resolved_timestamp)
			`,
			hostname,
			resolvedHostname)
		if err != nil {
			return log.Errorf("failed to insert %v into hostname_resolve: %v", hostname, err)
		}
		if hostname != resolvedHostname {
			// history is only interesting when there's actually something to resolve...
			_, err = db.ExecOrchestrator(`
			insert into  
					hostname_resolve_history (hostname, resolved_hostname, resolved_timestamp)
				values
					(?, ?, NOW())
				on duplicate key update 
					hostname=if(values(hostname) != resolved_hostname, values(hostname), hostname), 
					resolved_timestamp=values(resolved_timestamp)
			`,
				hostname,
				resolvedHostname)
			if err != nil {
				log.Errorf("failed to insert %v into hostname_resolve_history: %v", hostname, err)
			}
		}
		log.Debugf("WriteResolvedHostname: resolved %s to %s", hostname, resolvedHostname)
		writeResolvedHostnameCounter.Inc(1)
		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// ReadResolvedHostname returns the resolved hostname given a hostname, or empty if not exists
func ReadResolvedHostname(hostname string) (string, error) {
	var resolvedHostname string = ""

	query := `
		select 
			resolved_hostname
		from 
			hostname_resolve
		where
			hostname = ?
		`

	err := db.QueryOrchestrator(query, sqlutils.Args(hostname), func(m sqlutils.RowMap) error {
		resolvedHostname = m.GetString("resolved_hostname")
		return nil
	})
	readResolvedHostnameCounter.Inc(1)

	if err != nil {
		log.Errore(err)
	}
	return resolvedHostname, err
}

func readAllHostnameResolves() ([]HostnameResolve, error) {
	res := []HostnameResolve{}
	query := `
		select 
			hostname, 
			resolved_hostname  
		from 
			hostname_resolve
		`
	err := db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		hostnameResolve := HostnameResolve{hostname: m.GetString("hostname"), resolvedHostname: m.GetString("resolved_hostname")}

		res = append(res, hostnameResolve)
		return nil
	})
	readAllResolvedHostnamesCounter.Inc(1)

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

// readUnresolvedHostname reverse-reads hostname resolve. It returns a hostname which matches given pattern and resovles to resolvedHostname,
// or, in the event no such hostname is found, the given resolvedHostname, unchanged.
func readUnresolvedHostname(hostname string) (string, error) {
	unresolvedHostname := hostname

	query := `
	   		select
	   			unresolved_hostname
	   		from
	   			hostname_unresolve
	   		where
	   			hostname = ?
	   		`

	err := db.QueryOrchestrator(query, sqlutils.Args(hostname), func(m sqlutils.RowMap) error {
		unresolvedHostname = m.GetString("unresolved_hostname")
		return nil
	})
	readUnresolvedHostnameCounter.Inc(1)

	if err != nil {
		log.Errore(err)
	}
	return unresolvedHostname, err
}

// readMissingHostnamesToResolve gets those (unresolved, e.g. VIP) hostnames that *should* be present in
// the hostname_resolve table, but aren't.
func readMissingKeysToResolve() (result InstanceKeyMap, err error) {
	query := `
   		select 
   				hostname_unresolve.unresolved_hostname,
   				database_instance.port
   			from 
   				database_instance 
   				join hostname_unresolve on (database_instance.hostname = hostname_unresolve.hostname) 
   				left join hostname_resolve on (database_instance.hostname = hostname_resolve.resolved_hostname) 
   			where 
   				hostname_resolve.hostname is null
	   		`

	err = db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		instanceKey := InstanceKey{Hostname: m.GetString("unresolved_hostname"), Port: m.GetInt("port")}
		result.AddKey(instanceKey)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return result, err
}

// WriteHostnameUnresolve upserts an entry in hostname_unresolve
func WriteHostnameUnresolve(instanceKey *InstanceKey, unresolvedHostname string) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	insert into hostname_unresolve (
        		hostname,
        		unresolved_hostname,
        		last_registered)
        	values (?, ?, NOW())
        	on duplicate key update
        		unresolved_hostname=values(unresolved_hostname),
        		last_registered=now()
				`, instanceKey.Hostname, unresolvedHostname,
		)
		if err != nil {
			return log.Errore(err)
		}
		_, err = db.ExecOrchestrator(`
	        	replace into hostname_unresolve_history (
        		hostname,
        		unresolved_hostname,
        		last_registered)
        	values (?, ?, NOW())
				`, instanceKey.Hostname, unresolvedHostname,
		)
		writeUnresolvedHostnameCounter.Inc(1)
		return nil
	}
	return ExecDBWriteFunc(writeFunc)
}

// DeregisterHostnameUnresolve removes an unresovle entry
func DeregisterHostnameUnresolve(instanceKey *InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	delete from hostname_unresolve 
				where hostname=?
				`, instanceKey.Hostname,
		)
		return log.Errore(err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// ExpireHostnameUnresolve expires hostname_unresolve entries that haven't been updated recently.
func ExpireHostnameUnresolve() error {
	writeFunc := func() error {
		_, err := db.ExecOrchestrator(`
        	delete from hostname_unresolve 
				where last_registered < NOW() - INTERVAL ? MINUTE
				`, config.Config.ExpiryHostnameResolvesMinutes,
		)
		return log.Errorf("ExpireHostnameUnresolve: %v", err)
	}
	return ExecDBWriteFunc(writeFunc)
}

// ForgetExpiredHostnameResolves
func ForgetExpiredHostnameResolves() error {
	_, err := db.ExecOrchestrator(`
			delete 
				from hostname_resolve 
			where 
				resolved_timestamp < NOW() - interval (? * 2) minute`,
		config.Config.ExpiryHostnameResolvesMinutes,
	)
	return err
}

// DeleteInvalidHostnameResolves removes invalid resolves. At this time these are:
// - infinite loop resolves (A->B and B->A), remove earlier mapping
func DeleteInvalidHostnameResolves() error {
	var invalidHostnames []string

	query := `
		select 
		    early.hostname
		  from 
		    hostname_resolve as latest 
		    join hostname_resolve early on (latest.resolved_hostname = early.hostname and latest.hostname = early.resolved_hostname) 
		  where 
		    latest.hostname != latest.resolved_hostname 
		    and latest.resolved_timestamp > early.resolved_timestamp
	   	`

	err := db.QueryOrchestratorRowsMap(query, func(m sqlutils.RowMap) error {
		invalidHostnames = append(invalidHostnames, m.GetString("hostname"))
		return nil
	})
	if err != nil {
		return err
	}

	for _, invalidHostname := range invalidHostnames {
		_, err = db.ExecOrchestrator(`
			delete 
				from hostname_resolve 
			where 
				hostname = ?`,
			invalidHostname,
		)
		log.Errore(err)
	}
	return err
}

// deleteHostnameResolves compeltely erases the database cache
func deleteHostnameResolves() error {
	_, err := db.ExecOrchestrator(`
			delete 
				from hostname_resolve`,
	)
	return err
}
