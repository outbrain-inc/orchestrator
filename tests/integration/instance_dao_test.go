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
	"github.com/outbrain/orchestrator/go/config"
	"github.com/outbrain/orchestrator/go/db"
	. "gopkg.in/check.v1"
	"math/rand"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct{}

var _ = Suite(&TestSuite{})

// This test suite assumes one main and three direct subordinates, as follows;
// This was setup with mysqlsandbox (using MySQL 5.5.32, not that it matters) via:
// $ make_replication_sandbox --how_many_nodes=3 --replication_directory=55orchestrator /path/to/sandboxes/5.5.32
// modify below to fit your own environment
var mainKey = InstanceKey{
	Hostname: "127.0.0.1",
	Port:     22987,
}
var subordinate1Key = InstanceKey{
	Hostname: "127.0.0.1",
	Port:     22988,
}
var subordinate2Key = InstanceKey{
	Hostname: "127.0.0.1",
	Port:     22989,
}
var subordinate3Key = InstanceKey{
	Hostname: "127.0.0.1",
	Port:     22990,
}

func clearTestMaintenance() {
	_, _ = db.ExecOrchestrator("update database_instance_maintenance set maintenance_active=null, end_timestamp=NOW() where owner = ?", "unittest")
}

// The test also assumes one backend MySQL server.
func (s *TestSuite) SetUpSuite(c *C) {
	config.Config.MySQLTopologyUser = "msandbox"
	config.Config.MySQLTopologyPassword = "msandbox"
	config.Config.MySQLOrchestratorHost = "127.0.0.1"
	config.Config.MySQLOrchestratorPort = 5622
	config.Config.MySQLOrchestratorDatabase = "orchestrator"
	config.Config.MySQLOrchestratorUser = "msandbox"
	config.Config.MySQLOrchestratorPassword = "msandbox"
	config.Config.DiscoverByShowSubordinateHosts = true

	_, _ = db.ExecOrchestrator("delete from database_instance where hostname = ? and port = ?", mainKey.Hostname, mainKey.Port)
	_, _ = db.ExecOrchestrator("delete from database_instance where hostname = ? and port = ?", subordinate1Key.Hostname, subordinate1Key.Port)
	_, _ = db.ExecOrchestrator("delete from database_instance where hostname = ? and port = ?", subordinate2Key.Hostname, subordinate2Key.Port)
	_, _ = db.ExecOrchestrator("delete from database_instance where hostname = ? and port = ?", subordinate3Key.Hostname, subordinate3Key.Port)

	ExecInstance(&mainKey, "drop database if exists orchestrator_test")
	ExecInstance(&mainKey, "create database orchestrator_test")
	ExecInstance(&mainKey, `create table orchestrator_test.test_table(
			name    varchar(128) charset ascii not null primary key,
			value   varchar(128) charset ascii not null
		)`)
	rand.Seed(time.Now().UTC().UnixNano())
}

func (s *TestSuite) TestReadTopologyMain(c *C) {
	key := mainKey
	i, _ := ReadTopologyInstanceUnbuffered(&key)

	c.Assert(i.Key.Hostname, Equals, key.Hostname)
	c.Assert(i.IsSubordinate(), Equals, false)
	c.Assert(len(i.SubordinateHosts), Equals, 3)
	c.Assert(len(i.SubordinateHosts.GetInstanceKeys()), Equals, len(i.SubordinateHosts))
}

func (s *TestSuite) TestReadTopologySubordinate(c *C) {
	key := subordinate3Key
	i, _ := ReadTopologyInstanceUnbuffered(&key)
	c.Assert(i.Key.Hostname, Equals, key.Hostname)
	c.Assert(i.IsSubordinate(), Equals, true)
	c.Assert(len(i.SubordinateHosts), Equals, 0)
}

func (s *TestSuite) TestReadTopologyAndInstanceMain(c *C) {
	i, _ := ReadTopologyInstanceUnbuffered(&mainKey)
	iRead, found, _ := ReadInstance(&mainKey)
	c.Assert(found, Equals, true)
	c.Assert(iRead.Key.Hostname, Equals, i.Key.Hostname)
	c.Assert(iRead.Version, Equals, i.Version)
	c.Assert(len(iRead.SubordinateHosts), Equals, len(i.SubordinateHosts))
}

func (s *TestSuite) TestReadTopologyAndInstanceSubordinate(c *C) {
	i, _ := ReadTopologyInstanceUnbuffered(&subordinate1Key)
	iRead, found, _ := ReadInstance(&subordinate1Key)
	c.Assert(found, Equals, true)
	c.Assert(iRead.Key.Hostname, Equals, i.Key.Hostname)
	c.Assert(iRead.Version, Equals, i.Version)
}

func (s *TestSuite) TestGetMainOfASubordinate(c *C) {
	i, err := ReadTopologyInstanceUnbuffered(&subordinate1Key)
	c.Assert(err, IsNil)
	main, err := GetInstanceMain(i)
	c.Assert(err, IsNil)
	c.Assert(main.IsSubordinate(), Equals, false)
	c.Assert(main.Key.Port, Equals, 22987)
}

func (s *TestSuite) TestSubordinatesAreSiblings(c *C) {
	i0, _ := ReadTopologyInstanceUnbuffered(&subordinate1Key)
	i1, _ := ReadTopologyInstanceUnbuffered(&subordinate2Key)
	c.Assert(InstancesAreSiblings(i0, i1), Equals, true)
}

func (s *TestSuite) TestNonSiblings(c *C) {
	i0, _ := ReadTopologyInstanceUnbuffered(&mainKey)
	i1, _ := ReadTopologyInstanceUnbuffered(&subordinate1Key)
	c.Assert(InstancesAreSiblings(i0, i1), Not(Equals), true)
}

func (s *TestSuite) TestInstanceIsMainOf(c *C) {
	i0, _ := ReadTopologyInstanceUnbuffered(&mainKey)
	i1, _ := ReadTopologyInstanceUnbuffered(&subordinate1Key)
	c.Assert(InstanceIsMainOf(i0, i1), Equals, true)
}

func (s *TestSuite) TestStopStartSubordinate(c *C) {

	i, _ := ReadTopologyInstanceUnbuffered(&subordinate1Key)
	c.Assert(i.SubordinateRunning(), Equals, true)
	i, _ = StopSubordinateNicely(&i.Key, 0)

	c.Assert(i.SubordinateRunning(), Equals, false)
	c.Assert(i.SQLThreadUpToDate(), Equals, true)

	i, _ = StartSubordinate(&i.Key)
	c.Assert(i.SubordinateRunning(), Equals, true)
}

func (s *TestSuite) TestReadTopologyUnexisting(c *C) {
	key := InstanceKey{
		Hostname: "127.0.0.1",
		Port:     22999,
	}
	_, err := ReadTopologyInstanceUnbuffered(&key)

	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestMoveBelowAndBack(c *C) {
	clearTestMaintenance()
	// become child
	subordinate1, err := MoveBelow(&subordinate1Key, &subordinate2Key)
	c.Assert(err, IsNil)

	c.Assert(subordinate1.MainKey.Equals(&subordinate2Key), Equals, true)
	c.Assert(subordinate1.SubordinateRunning(), Equals, true)

	// And back; keep topology intact
	subordinate1, _ = MoveUp(&subordinate1Key)
	subordinate2, _ := ReadTopologyInstanceUnbuffered(&subordinate2Key)

	c.Assert(InstancesAreSiblings(subordinate1, subordinate2), Equals, true)
	c.Assert(subordinate1.SubordinateRunning(), Equals, true)

}

func (s *TestSuite) TestMoveBelowAndBackComplex(c *C) {
	clearTestMaintenance()

	// become child
	subordinate1, _ := MoveBelow(&subordinate1Key, &subordinate2Key)

	c.Assert(subordinate1.MainKey.Equals(&subordinate2Key), Equals, true)
	c.Assert(subordinate1.SubordinateRunning(), Equals, true)

	// Now let's have fun. Stop subordinate2 (which is now parent of subordinate1), execute queries on main,
	// move s1 back under main, start all, verify queries.

	_, err := StopSubordinate(&subordinate2Key)
	c.Assert(err, IsNil)

	randValue := rand.Int()
	_, err = ExecInstance(&mainKey, `replace into orchestrator_test.test_table (name, value) values ('TestMoveBelowAndBackComplex', ?)`, randValue)
	c.Assert(err, IsNil)
	main, err := ReadTopologyInstanceUnbuffered(&mainKey)
	c.Assert(err, IsNil)

	// And back; keep topology intact
	subordinate1, err = MoveUp(&subordinate1Key)
	c.Assert(err, IsNil)
	_, err = MainPosWait(&subordinate1Key, &main.SelfBinlogCoordinates)
	c.Assert(err, IsNil)
	subordinate2, err := ReadTopologyInstanceUnbuffered(&subordinate2Key)
	c.Assert(err, IsNil)
	_, err = MainPosWait(&subordinate2Key, &main.SelfBinlogCoordinates)
	c.Assert(err, IsNil)
	// Now check for value!
	var value1, value2 int
	ScanInstanceRow(&subordinate1Key, `select value from orchestrator_test.test_table where name='TestMoveBelowAndBackComplex'`, &value1)
	ScanInstanceRow(&subordinate2Key, `select value from orchestrator_test.test_table where name='TestMoveBelowAndBackComplex'`, &value2)

	c.Assert(InstancesAreSiblings(subordinate1, subordinate2), Equals, true)
	c.Assert(value1, Equals, randValue)
	c.Assert(value2, Equals, randValue)
}

func (s *TestSuite) TestFailMoveBelow(c *C) {
	clearTestMaintenance()
	_, _ = ExecInstance(&subordinate2Key, `set global binlog_format:='ROW'`)
	_, err := MoveBelow(&subordinate1Key, &subordinate2Key)
	_, _ = ExecInstance(&subordinate2Key, `set global binlog_format:='STATEMENT'`)
	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestMakeCoMainAndBack(c *C) {
	clearTestMaintenance()

	subordinate1, err := MakeCoMain(&subordinate1Key)
	c.Assert(err, IsNil)

	// Now main & subordinate1 expected to be co-mains. Check!
	main, _ := ReadTopologyInstanceUnbuffered(&mainKey)
	c.Assert(main.IsSubordinateOf(subordinate1), Equals, true)
	c.Assert(subordinate1.IsSubordinateOf(main), Equals, true)

	// reset - restore to original state
	main, err = ResetSubordinateOperation(&mainKey)
	subordinate1, _ = ReadTopologyInstanceUnbuffered(&subordinate1Key)
	c.Assert(err, IsNil)
	c.Assert(main.MainKey.Hostname, Equals, "_")
}

func (s *TestSuite) TestFailMakeCoMain(c *C) {
	clearTestMaintenance()
	_, err := MakeCoMain(&mainKey)
	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestMakeCoMainAndBackAndFailOthersToBecomeCoMains(c *C) {
	clearTestMaintenance()

	subordinate1, err := MakeCoMain(&subordinate1Key)
	c.Assert(err, IsNil)

	// Now main & subordinate1 expected to be co-mains. Check!
	main, _, _ := ReadInstance(&mainKey)
	c.Assert(main.IsSubordinateOf(subordinate1), Equals, true)
	c.Assert(subordinate1.IsSubordinateOf(main), Equals, true)

	// Verify can't have additional co-mains
	_, err = MakeCoMain(&mainKey)
	c.Assert(err, Not(IsNil))
	_, err = MakeCoMain(&subordinate1Key)
	c.Assert(err, Not(IsNil))
	_, err = MakeCoMain(&subordinate2Key)
	c.Assert(err, Not(IsNil))

	// reset subordinate - restore to original state
	main, err = ResetSubordinateOperation(&mainKey)
	c.Assert(err, IsNil)
	c.Assert(main.MainKey.Hostname, Equals, "_")
}

func (s *TestSuite) TestDiscover(c *C) {
	var err error
	_, err = db.ExecOrchestrator("delete from database_instance where hostname = ? and port = ?", mainKey.Hostname, mainKey.Port)
	_, err = db.ExecOrchestrator("delete from database_instance where hostname = ? and port = ?", subordinate1Key.Hostname, subordinate1Key.Port)
	_, err = db.ExecOrchestrator("delete from database_instance where hostname = ? and port = ?", subordinate2Key.Hostname, subordinate2Key.Port)
	_, err = db.ExecOrchestrator("delete from database_instance where hostname = ? and port = ?", subordinate3Key.Hostname, subordinate3Key.Port)
	_, found, _ := ReadInstance(&mainKey)
	c.Assert(found, Equals, false)
	_, _ = ReadTopologyInstanceUnbuffered(&subordinate1Key)
	_, found, err = ReadInstance(&subordinate1Key)
	c.Assert(found, Equals, true)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestForgetMain(c *C) {
	_, _ = ReadTopologyInstanceUnbuffered(&mainKey)
	_, found, _ := ReadInstance(&mainKey)
	c.Assert(found, Equals, true)
	ForgetInstance(&mainKey)
	_, found, _ = ReadInstance(&mainKey)
	c.Assert(found, Equals, false)
}

func (s *TestSuite) TestBeginMaintenance(c *C) {
	clearTestMaintenance()
	_, _ = ReadTopologyInstanceUnbuffered(&mainKey)
	_, err := BeginMaintenance(&mainKey, "unittest", "TestBeginMaintenance")

	c.Assert(err, IsNil)
}

func (s *TestSuite) TestBeginEndMaintenance(c *C) {
	clearTestMaintenance()
	_, _ = ReadTopologyInstanceUnbuffered(&mainKey)
	k, err := BeginMaintenance(&mainKey, "unittest", "TestBeginEndMaintenance")
	c.Assert(err, IsNil)
	err = EndMaintenance(k)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestFailBeginMaintenanceTwice(c *C) {
	clearTestMaintenance()
	_, _ = ReadTopologyInstanceUnbuffered(&mainKey)
	_, err := BeginMaintenance(&mainKey, "unittest", "TestFailBeginMaintenanceTwice")
	c.Assert(err, IsNil)
	_, err = BeginMaintenance(&mainKey, "unittest", "TestFailBeginMaintenanceTwice")
	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestFailEndMaintenanceTwice(c *C) {
	clearTestMaintenance()
	_, _ = ReadTopologyInstanceUnbuffered(&mainKey)
	k, err := BeginMaintenance(&mainKey, "unittest", "TestFailEndMaintenanceTwice")
	c.Assert(err, IsNil)
	err = EndMaintenance(k)
	c.Assert(err, IsNil)
	err = EndMaintenance(k)
	c.Assert(err, Not(IsNil))
}

func (s *TestSuite) TestFailMoveBelowUponMaintenance(c *C) {
	clearTestMaintenance()
	_, _ = ReadTopologyInstanceUnbuffered(&subordinate1Key)
	k, err := BeginMaintenance(&subordinate1Key, "unittest", "TestBeginEndMaintenance")
	c.Assert(err, IsNil)

	_, err = MoveBelow(&subordinate1Key, &subordinate2Key)
	c.Assert(err, Not(IsNil))

	err = EndMaintenance(k)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestFailMoveBelowUponSubordinateStopped(c *C) {
	clearTestMaintenance()

	subordinate1, _ := ReadTopologyInstanceUnbuffered(&subordinate1Key)
	c.Assert(subordinate1.SubordinateRunning(), Equals, true)
	subordinate1, _ = StopSubordinateNicely(&subordinate1.Key, 0)
	c.Assert(subordinate1.SubordinateRunning(), Equals, false)

	_, err := MoveBelow(&subordinate1Key, &subordinate2Key)
	c.Assert(err, Not(IsNil))

	_, _ = StartSubordinate(&subordinate1.Key)
}

func (s *TestSuite) TestFailMoveBelowUponOtherSubordinateStopped(c *C) {
	clearTestMaintenance()

	subordinate1, _ := ReadTopologyInstanceUnbuffered(&subordinate1Key)
	c.Assert(subordinate1.SubordinateRunning(), Equals, true)
	subordinate1, _ = StopSubordinateNicely(&subordinate1.Key, 0)
	c.Assert(subordinate1.SubordinateRunning(), Equals, false)

	_, err := MoveBelow(&subordinate2Key, &subordinate1Key)
	c.Assert(err, Not(IsNil))

	_, _ = StartSubordinate(&subordinate1.Key)
}
