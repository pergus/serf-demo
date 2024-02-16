package main

/*
./serf -bindAddr 127.0.0.1 -bindPort 6666 -advertiseAddr 127.0.0.1 -advertisePort 6666 -clusterAddr 127.0.0.1 -clusterPort 6666 -name a1
./serf -bindAddr 127.0.0.1 -bindPort 7777 -advertiseAddr 127.0.0.1 -advertisePort 7777 -clusterAddr 127.0.0.1 -clusterPort 6666 -name a2
*/

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/hashicorp/logutils"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
)

// ClusterConfig holds the configuration parameters for setting up a Serf cluster.
type ClusterConfig struct {
	BindAddr      string
	BindPort      string
	AdvertiseAddr string
	AdvertisePort string
	ClusterAddr   string
	ClusterPort   string
	Name          string
}

var events chan serf.Event

// CRDT represents a simple Counter CRDT.
type CRDT struct {
	mu      sync.Mutex
	counter int
}

// setupCluster initializes and joins a Serf cluster.
func setupCluster(config ClusterConfig) (*serf.Serf, *CRDT, error) {

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR"},
		MinLevel: logutils.LogLevel("INFO"),
		Writer:   os.Stderr,
	}

	// Initialize Serf configuration
	conf := serf.DefaultConfig()
	conf.Init()
	conf.MemberlistConfig.LogOutput = filter
	conf.LogOutput = filter

	// Set Memberlist configuration
	conf.MemberlistConfig.AdvertiseAddr = config.AdvertiseAddr
	conf.MemberlistConfig.AdvertisePort, _ = strconv.Atoi(config.AdvertisePort)
	conf.MemberlistConfig.BindAddr = config.BindAddr
	conf.MemberlistConfig.BindPort, _ = strconv.Atoi(config.BindPort)
	conf.MemberlistConfig.ProtocolVersion = 3 // Version 3 enables binding different ports for each agent
	conf.NodeName = config.Name
	events = make(chan serf.Event)
	conf.EventCh = events

	// Create a Serf cluster
	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Couldn't create cluster")
	}

	// Join the cluster with the specified address and port
	_, err = cluster.Join([]string{config.ClusterAddr + ":" + config.ClusterPort}, true)
	if err != nil {
		log.Printf("Couldn't join cluster, starting own: %v\n", err)
	}

	// Create a Counter CRDT
	counterCRDT := &CRDT{}

	return cluster, counterCRDT, nil
}

// main function
func main() {

	// Parse command-line flags for cluster configuration
	config := parseFlags()

	// Set up the Serf cluster and CRDT
	cluster, counterCRDT, err := setupCluster(config)
	if err != nil {
		log.Fatal(err)
	}

	// Set up a signal channel to handle interrupts and termination signals
	go waitForSignal()

	// Start the REPL for interacting with the Serf cluster
	go startREPL(cluster, counterCRDT)

	// Handle user and member events
	handleUserEvents(cluster, counterCRDT)
}

// parseFlags parses command-line flags and returns a ClusterConfig.
func parseFlags() ClusterConfig {
	var config ClusterConfig

	flag.StringVar(&config.BindAddr, "bindAddr", "127.0.0.1", "Address for the agent to listen for incoming connections")
	flag.StringVar(&config.BindPort, "bindPort", "6666", "Port for the agent to listen for incoming connections")
	flag.StringVar(&config.AdvertiseAddr, "advertiseAddr", "127.0.0.1", "Address for the agent to be reachable")
	flag.StringVar(&config.AdvertisePort, "advertisePort", "6666", "Port for the agent to be reachable")
	flag.StringVar(&config.ClusterAddr, "clusterAddr", "127.0.0.1", "Address of the first agent in the cluster")
	flag.StringVar(&config.ClusterPort, "clusterPort", "6666", "Port of the first agent in the cluster")
	flag.StringVar(&config.Name, "name", "default", "Unique name for the agent in the cluster")

	flag.Parse()

	return config
}

// waitForSignal sets up a signal channel to handle interrupts and termination signals.
func waitForSignal() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}

// startREPL initiates the REPL for interacting with the Serf cluster.
func startREPL(cluster *serf.Serf, crdt *CRDT) {
	fmt.Println("Welcome to the Serf Cluster REPL. Type 'help' for available commands.")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		scanner.Scan()
		command := scanner.Text()

		switch command {
		case "help":
			fmt.Println("Available commands:")
			fmt.Println("  list - List all servers connected to the cluster")
			fmt.Println("  edit-crdt - Increment the counter CRDT and distribute it to all nodes")
			fmt.Println("  show-crdt - Show the current value of the counter CRDT")
			fmt.Println("  exit - Exit the REPL")
		case "list":
			listMembers(cluster)
		case "edit-crdt":
			editCRDT(cluster, crdt)
		case "show-crdt":
			showCRDT(crdt)
		case "exit":
			fmt.Println("Exiting the REPL. Leaving the Serf cluster gracefully.")
			cluster.Leave()
			os.Exit(0)
		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

// handleUserEvents listens for user events and processes them.
func handleUserEvents(cluster *serf.Serf, crdt *CRDT) {
	for {
		select {
		case e := <-events:
			switch e.EventType() {
			case serf.EventMemberJoin, serf.EventMemberLeave, serf.EventMemberFailed:
				handleMemberUpdate(e, crdt)
			case serf.EventUser:
				userEvent := e.(serf.UserEvent)
				handleUserEvent(userEvent, crdt)
			}
		}
	}
}

// handleMemberUpdate processes member updates.
func handleMemberUpdate(event serf.Event, crdt *CRDT) {
	switch event.EventType() {
	case serf.EventMemberJoin:
		member := event.(serf.MemberEvent).Members[0]
		fmt.Printf("Member joined: %s\n", member.Name)
	case serf.EventMemberLeave, serf.EventMemberFailed:
		member := event.(serf.MemberEvent).Members[0]
		fmt.Printf("Member left or failed: %s\n", member.Name)
		// You may want to handle CRDT adjustment or other actions here if needed
	}
}

// handleUserEvent processes the received user event.
func handleUserEvent(event serf.UserEvent, crdt *CRDT) {
	if event.Name == "crdt-update" {
		payload := event.Payload
		value, err := strconv.Atoi(string(payload))
		if err != nil {
			fmt.Printf("Error converting CRDT update payload: %v\n", err)
			return
		}

		crdt.setValue(value)
		fmt.Printf("Received CRDT update. New value: %d\n", value)
	}
}

// MarshalTags is a utility function which takes a map of tag key/value pairs
// and returns the same tags as strings in 'key=value' format.
func MarshalTags(tags map[string]string) []string {
	var result []string
	for name, value := range tags {
		result = append(result, fmt.Sprintf("%s=%s", name, value))
	}
	return result
}

// UnmarshalTags is a utility function which takes a slice of strings in
// key=value format and returns them as a tag mapping.
func UnmarshalTags(tags []string) (map[string]string, error) {
	result := make(map[string]string)
	for _, tag := range tags {
		parts := strings.SplitN(tag, "=", 2)
		if len(parts) != 2 || len(parts[0]) == 0 {
			return nil, fmt.Errorf("Invalid tag: '%s'", tag)
		}
		result[parts[0]] = parts[1]
	}
	return result, nil
}

// listMembers prints a list of all servers connected to the cluster.
func listMembers(cluster *serf.Serf) {
	members := cluster.Members()
	fmt.Println("Connected Servers:")
	for _, member := range members {
		if member.Status != serf.StatusLeft {

			fmt.Printf("  %s %s %s\n", member.Name, member.Addr.String(), MarshalTags(member.Tags))
		}
	}
}

// showCRDT prints the current value of the counter CRDT.
func showCRDT(crdt *CRDT) {
	value := crdt.getValue()
	fmt.Printf("Current value of the counter CRDT: %d\n", value)
}

// editCRDT increments the counter CRDT and distributes the update to all nodes.
func editCRDT(cluster *serf.Serf, crdt *CRDT) {
	crdt.increment()
	fmt.Printf("Counter CRDT incremented. New value: %d\n", crdt.getValue())

	// Broadcast the updated CRDT to all nodes in the cluster
	broadcastUpdate(cluster, crdt)
}

// broadcastUpdate sends a broadcast message to all nodes in the cluster with the updated CRDT value.
func broadcastUpdate(cluster *serf.Serf, crdt *CRDT) {
	event := serf.UserEvent{
		Name:    "crdt-update",
		Payload: []byte(strconv.Itoa(crdt.getValue())),
	}

	err := cluster.UserEvent(event.Name, event.Payload, true)
	if err != nil {
		fmt.Printf("Error broadcasting CRDT update: %v\n", err)
	} else {
		fmt.Println("CRDT update broadcasted successfully.")
	}
}

// CRDT methods
func (c *CRDT) increment() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter++
}

func (c *CRDT) setValue(value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter = value
}

func (c *CRDT) getValue() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counter
}
