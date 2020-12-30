package mydynamotest

import (
	"testing"
	"time"
	"fmt"
)

func TestReplace(t *testing.T) {
	t.Logf("Starting Replace test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance := MakeConnectedClient(8080)

	//Put a value on key "s1"
	context := PutFreshContext("s1", []byte("abcde"))
	clientInstance.Put(context)

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestReplace: Returned nil")
	}
	fmt.Println("GOTVALUEPTR VAL: ", gotValuePtr.EntryList[0].Value)
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestReplace: Failed to get value 1")
	}


	context.Context.Clock.Increment("0")
	context.Value = []byte("edcba")
	//Put a value on key "s1"
	clientInstance.Put(context)

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestReplace: Returned nil")
	}
	fmt.Println("GOTVALUEPTR VAL: ", gotValuePtr.EntryList[0].Value)
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("edcba")) {
		t.Fail()
		t.Logf("TestReplace: Failed to get value 2")
		t.Logf("TestReplace: Value: " + string(gotValue.EntryList[0].Value))
	}
}

func TestCrash(t *testing.T) {

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance := MakeConnectedClient(8080)

	c := PutFreshContext("k1", []byte("abcde"))
	//Put a value on key "s1"
	clientInstance.Crash(5)
	e := clientInstance.Put(c)

	if e {		
		t.Fail()
		t.Logf("TestCrash: Returned nil")
	}

}

func TestCrash2(t *testing.T) {

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance := MakeConnectedClient(8080)
	clientInstance2 := MakeConnectedClient(8081)
	clientInstance3 := MakeConnectedClient(8082)

	c := PutFreshContext("k1", []byte("abcde"))
	//Put a value on key "s1"
	e := clientInstance.Put(c)
	if !e {		
		t.Fail()
		t.Logf("TestCrash2: Could not put")
	}
	clientInstance.Crash(2)
	clientInstance.Gossip()

	e2 := clientInstance2.Get("k1")
	if e2 != nil {		
		t.Fail()
		t.Logf("TestCrash2: Returned nil")
	}

	e3 := clientInstance3.Get("k1")
	if e3 != nil {		
		t.Fail()
		t.Logf("TestCrash2: Returned nil")
	}


	time.Sleep(2 * time.Second)
	clientInstance.Gossip()


	e2 = clientInstance2.Get("k1")
	if e2 == nil {		
		t.Fail()
		t.Logf("TestCrash2: Returned nil")
	}

	e3 = clientInstance3.Get("k1")
	if e3 == nil {		
		t.Fail()
		t.Logf("TestCrash2: Returned nil")
	}
}

func TestConflict(t *testing.T) {
	t.Logf("Starting Replace test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	clientInstance := MakeConnectedClient(8080)

	c := PutFreshContext("k1", []byte("abcde"))
	//Put a value on key "s1"
	clientInstance.Put(c)

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("k1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestConflict: Returned nil")
	}
	fmt.Println("GOTVALUEPTR VAL: ", gotValuePtr.EntryList[0].Value)
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestConflict: Failed to get value 1")
	}


	fmt.Println("C Clock: ", c.Context.Clock.Clocks)
	c = PutFreshContext("k1", []byte("edcba"))
	c.Context.Clock.Increment("0")
	c.Context.Clock.Increment("0")

	//Put a value on key "s1"
	clientInstance.Put(c)

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance.Get("k1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestConflict: Returned nil")
	}
	fmt.Println("GOTVALUEPTR VAL: ", gotValuePtr.EntryList[0].Value)
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("edcba")) {
		t.Fail()
		t.Logf("TestConflict: Failed to get value 2")
		fmt.Println("S")
		fmt.Println(gotValue.EntryList[0].Value)
		fmt.Println("E")
		fmt.Println(gotValue.EntryList[0].Context.Clock.Clocks)
	}


	clientInstanceY := MakeConnectedClient(8081)
	clientInstanceZ := MakeConnectedClient(8082)

	clientInstance.Gossip()

	//Put a value on key "s1"
	clientInstanceY.Put(PutFreshContext("s1", []byte("1233")))
	//Put a value on key "s1"
	clientInstanceZ.Put(PutFreshContext("s1", []byte("5476")))

	clientInstanceY.Gossip()
	clientInstanceZ.Gossip()	

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr = clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestConflict: Returned nil")
	}
	fmt.Println("GOTVALUEPTR VAL: ", gotValuePtr.EntryList[0].Value)
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 2 || (!valuesEqual(gotValue.EntryList[0].Value, []byte("1233")) && !valuesEqual(gotValue.EntryList[1].Value, []byte("1233"))) || (!valuesEqual(gotValue.EntryList[0].Value, []byte("5476")) && !valuesEqual(gotValue.EntryList[1].Value, []byte("5476"))) {
		t.Fail()
		t.Logf("TestConflict: Failed to get value 3")
		fmt.Println("gotValue.EntryList")
		fmt.Println(gotValue.EntryList)
		fmt.Println("gotValue.EntryList")
	}

}