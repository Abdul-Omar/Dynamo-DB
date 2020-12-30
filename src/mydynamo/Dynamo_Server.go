package mydynamo

import (
	"net"
	"net/http"
	"net/rpc"
	"errors"
	"time"
	"bytes"
	"sync"
	"log"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	data map[string][]ObjectEntry       // local strogage for this node
	gossipList map[string][]int
	crashed *bool
	m *sync.Mutex
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	if (*s.crashed) {
		return errors.New("Server crashed")
	}
	s.preferenceList = incomingList
	return nil
}


func (s *DynamoServer) GetNodeID(_ Empty, id *string) error {
	*id = s.nodeID
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	if (*s.crashed) {
		return errors.New("Server crashed")
	}

	s.m.Lock()

	for key, _ := range s.gossipList {
		for i := 0; i < len(s.gossipList[key]); i++ {
			node := s.gossipList[key][i]
			address := (*s).preferenceList[node].Address
			port := (*s).preferenceList[node].Port
			var res bool
			c, err := rpc.DialHTTP("tcp", address+":"+ port)			
			if err != nil {
				continue
			}
			var empty Empty
			var id string
			err2 := c.Call("MyDynamo.GetNodeID", empty, &id)
			if err2 != nil {
				continue
			}

			if id == s.nodeID {
				continue
			}

			s.m.Unlock()
			for _, elem := range s.data[key] {
				var value PutArgs
				value.Key = key
				value.Context = elem.Context
				value.Value = elem.Value
				err2 := c.Call("MyDynamo.GossipPut", value, &res)
				if err2 != nil {
					continue
				} 
			}
			s.m.Lock()
			s.gossipList[key] = append(s.gossipList[key][:i], s.gossipList[key][i+1:]...)
			i--
		}
	}

	s.m.Unlock()
	return nil
}

func (s *DynamoServer) EndCrash(seconds int,  _ *Empty) error {
	for {
		if (seconds <= 0) {
			break
		}
		time.Sleep(1 * time.Second)
		seconds -= 1
	}
	*(s.crashed) = false

	return nil
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	if (*s.crashed) {
		return errors.New("Server crashed")
	}

	*(s.crashed) = true
	go (*s).EndCrash(seconds, &Empty{})

	return nil
}


// Put a file to this server and W other servers
func (s *DynamoServer) GossipPut(value PutArgs, result *bool) error {
	if (*s.crashed) {
		return errors.New("Server crashed")
	}

	s.m.Lock()
	if len(s.data[value.Key]) < 1 || s.data[value.Key][0].Context.Clock.Concurrent(value.Context.Clock) {
		s.data[value.Key] = append(s.data[value.Key], ObjectEntry{
			Context: value.Context,
			Value: value.Value,
		})			
		//value.Context.Clock.Clocks[val] += 1
		s.m.Unlock()
		return nil
	} else if s.data[value.Key][0].Context.Clock.LessThan(value.Context.Clock) && !s.data[value.Key][0].Context.Clock.Equals(value.Context.Clock){
		var o ObjectEntry
		o.Context = value.Context
		o.Value = value.Value
		l := make([]ObjectEntry, 0)
		s.data[value.Key] = append(l, o)
	}

	s.m.Unlock()
	return nil
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put2(value PutArgs, result *bool) error {
	if (*s.crashed) {
		return errors.New("Server crashed")
	}
	s.m.Lock()

	if len(s.data[value.Key]) < 1 {
		var o ObjectEntry
		o.Context = value.Context
		o.Value = value.Value
		l := make([]ObjectEntry, 0)
		s.data[value.Key] = append(l, o)
		s.m.Unlock()
		return nil
	}

	for i := 0; i < len(s.data[value.Key]); i++ {
		if s.data[value.Key][i].Context.Clock.LessThan(value.Context.Clock) && !s.data[value.Key][i].Context.Clock.Equals(value.Context.Clock){
			s.data[value.Key] = append(s.data[value.Key][:i], s.data[value.Key][i+1:]...)
			i--
		}
	}
	var o ObjectEntry
	o.Context = value.Context
	o.Value = value.Value
	s.data[value.Key] = append(s.data[value.Key], o)

	s.m.Unlock()
	return nil
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {
	if (*s.crashed) {
		return errors.New("Server crashed")
	}

	val, exists := (*s).data[value.Key]
	value.Context.Clock.Increment(s.nodeID)

	for k := 0; k < len(s.data[value.Key]); k++ {	
		if exists && ((!val[k].Context.Clock.Concurrent(value.Context.Clock) && value.Context.Clock.LessThan(val[k].Context.Clock))) {
			return errors.New("Invalid context")
		}
	}


	j := 0
	i := 0
	gL := make([]int, 0)

	//Call put on w nodes and create gossip list of failed puts
	var r bool
	s.Put2(value, &r)
	for {
		if (i >= len((*s).preferenceList) || j >= (s.wValue - 1)) {
			break
		}
		address := (*s).preferenceList[i].Address
		port := (*s).preferenceList[i].Port
		var res bool
		c, err := rpc.DialHTTP("tcp", address+":"+ port)
		if err != nil {
			gL = append(gL, i)
			return errors.New("Failed to send preference list")
		}
		var empty Empty
		var id string
		err2 := c.Call("MyDynamo.GetNodeID", empty, &id)
		if err2 != nil {
			gL = append(gL, i)
			return errors.New("Failed to call get node id")
		} else if id == s.nodeID {
			i++
			continue
		}
		err3 := c.Call("MyDynamo.Put2", value, &res)
		if err3 != nil {
			gL = append(gL, i)
			return errors.New("Failed to call put on node in preference")
		} else {
			j += 1
		}
		i += 1
	}

	//Add nodes not called to gossipList
	for {
		if (i >= len((*s).preferenceList)) {
			break
		}
		gL = append(gL, i)
		i += 1
	}

	s.m.Lock()
	(*s).gossipList[value.Key] = gL
	s.m.Unlock()
	*result = true
	return nil

}

func (s *DynamoServer) Get2(key string, obj *[]ObjectEntry) error {
	if (*s.crashed) {
		return errors.New("Server crashed")
	}
	_, exists := (*s).data[key]

	if !exists {
		return errors.New("Does not exist") 
	}

	*obj = (*s).data[key]
	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	if (*s.crashed) {
		return errors.New("Server crashed")
	}

	var objList []ObjectEntry
	e := s.Get2(key, &objList)
	if e != nil {
		//objList = obj
		return errors.New("Could not Get")
	}
	j := 0
	for i := 0; i < len(s.preferenceList) && j < (s.rValue - 1); i++ {
		address := (*s).preferenceList[i].Address
		port := (*s).preferenceList[i].Port	
		var object []ObjectEntry
		c, err := rpc.DialHTTP("tcp", address+":"+ port)
		if err != nil {
			return errors.New("Failed to send preference list")
		} 	
		var id string
		var empty Empty
		err2 := c.Call("MyDynamo.GetNodeID", empty, &id)
		if err2 != nil  {
			return errors.New("Failed to call get node id")
		} else if id == s.nodeID {
			continue
		}

		err2 = c.Call("MyDynamo.Get2", key, &object)
		if err2 != nil {
			continue
		} else {
			for j := 0; j < len(object); j++ {
				log.Println("Current objList")
				log.Println(objList)
				log.Println("Got from Node ", (*s).preferenceList[i].Port)
				log.Println("object")
				log.Println(object)
				equal := false
				for k := 0; k < len(objList); k++ {
					if objList[k].Context.Clock.LessThan(object[j].Context.Clock) && !objList[k].Context.Clock.Equals(object[j].Context.Clock){
						objList = append(objList[:k], objList[k+1:]...)
						k--
					} else if objList[k].Context.Clock.Equals(object[j].Context.Clock) && bytes.Compare(objList[k].Value, object[j].Value) == 0 {
						equal = true
						break
					}
				}
				if !equal && len(object[j].Value) > 0 {
					objList = append(objList, object[j])
				}
			}
		}
		j++	
	}

	(*result) = DynamoResult {
		EntryList: objList,
	}
	
	return nil
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
 	var c bool
 	c = false
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		data : make(map[string][]ObjectEntry),
		gossipList: make(map[string][]int),
		crashed: &c,
		m: new(sync.Mutex),
	}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
