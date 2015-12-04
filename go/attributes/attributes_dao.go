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

package attributes

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/outbrain/orchestrator/Godeps/_workspace/src/github.com/outbrain/golib/log"
	"github.com/outbrain/orchestrator/Godeps/_workspace/src/github.com/outbrain/golib/sqlutils"
	"github.com/outbrain/orchestrator/go/db"
)

func readResponse(res *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.Status == "500" {
		return body, errors.New("Response Status 500")
	}

	return body, nil
}

// SetHostAttributes
func SetHostAttributes(hostname string, attributeName string, attributeValue string) error {
	_, err := db.ExecOrchestrator(`
			replace 
				into host_attributes (
					hostname, attribute_name, attribute_value, submit_timestamp, expire_timestamp
				) VALUES (
					?, ?, ?, NOW(), NULL
				)
			`,
		hostname,
		attributeName,
		attributeValue,
	)
	if err != nil {
		return log.Errore(err)
	}

	return err
}

func getHostAttributesByClause(whereClause string, args []interface{}) ([]HostAttributes, error) {
	res := []HostAttributes{}
	query := fmt.Sprintf(`
		select 
			hostname, 
			attribute_name, 
			attribute_value,
			submit_timestamp ,
			ifnull(expire_timestamp, '') as expire_timestamp  
		from 
			host_attributes
		%s
		order by
			hostname, attribute_name
		`, whereClause)

	err := db.QueryOrchestrator(query, args, func(m sqlutils.RowMap) error {
		hostAttributes := HostAttributes{}
		hostAttributes.Hostname = m.GetString("hostname")
		hostAttributes.AttributeName = m.GetString("attribute_name")
		hostAttributes.AttributeValue = m.GetString("attribute_value")
		hostAttributes.SubmitTimestamp = m.GetString("submit_timestamp")
		hostAttributes.ExpireTimestamp = m.GetString("expire_timestamp")

		res = append(res, hostAttributes)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

// GetHostAttributesByMatch
func GetHostAttributesByMatch(hostnameMatch string, attributeNameMatch string, attributeValueMatch string) ([]HostAttributes, error) {
	terms := []string{}
	args := sqlutils.Args()
	if hostnameMatch != "" {
		terms = append(terms, ` hostname rlike ? `)
		args = append(args, hostnameMatch)
	}
	if attributeNameMatch != "" {
		terms = append(terms, ` attribute_name rlike ? `)
		args = append(args, attributeNameMatch)
	}
	if attributeValueMatch != "" {
		terms = append(terms, ` attribute_value rlike ? `)
		args = append(args, attributeValueMatch)
	}

	if len(terms) == 0 {
		return getHostAttributesByClause("", args)
	}
	whereCondition := fmt.Sprintf(" where %s ", strings.Join(terms, " and "))

	return getHostAttributesByClause(whereCondition, args)
}

// GetHostAttributesByMatch
func GetHostAttributesByAttribute(attributeName string, valueMatch string) ([]HostAttributes, error) {
	if valueMatch == "" {
		valueMatch = ".?"
	}
	whereClause := ` where attribute_name = ? and attribute_value rlike ?`

	return getHostAttributesByClause(whereClause, sqlutils.Args(attributeName, valueMatch))
}
