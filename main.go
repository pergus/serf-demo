/*
serf-demo -bindAddr 127.0.0.1 -bindPort 6666 -name a1
serf-demo -bindAddr 127.0.0.1 -bindPort 7777 -clusterAddr 127.0.0.1 -clusterPort 6666 -name a2

alt.

serf-demo -name a1
Use the info command on a1 to get the cluster port for the next command:
serf-demo -name a2 -clusterPort 57158

*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"serf-demo/table"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	"github.com/hashicorp/logutils"
	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
)

// ClusterConfig holds the configuration parameters for setting up a Serf cluster.
type ClusterConfig struct {
	BindAddr    string
	BindPort    string
	ClusterAddr string
	ClusterPort string
	Name        string
}

var events chan serf.Event   // Surf events
var exitSignal chan struct{} // Channel to signal exit

// CRDT represents a simple Counter CRDT.
type CRDT struct {
	mu    sync.Mutex
	value int
	clock int
}

// setupCluster initializes and joins a Serf cluster.
func setupCluster(config ClusterConfig) (*serf.Serf, *CRDT, error) {

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR"},
		MinLevel: logutils.LogLevel("ERROR"),
		Writer:   os.Stderr,
	}

	// Initialize Serf configuration
	conf := serf.DefaultConfig()
	conf.Init()
	conf.MemberlistConfig.LogOutput = filter
	conf.LogOutput = filter

	// Set Memberlist configuration
	conf.MemberlistConfig.AdvertiseAddr = config.BindAddr
	conf.MemberlistConfig.AdvertisePort, _ = strconv.Atoi(config.BindPort)

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

func IPv4() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddress := conn.LocalAddr().(*net.UDPAddr)

	return localAddress.IP
}

func Hostname() string {
	name, err := os.Hostname()
	if err != nil {
		return ""
	}

	return name
}

// availablePort requests an available port from the OS
func availablePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0
	}
	defer l.Close()
	addr := l.Addr().(*net.TCPAddr)
	return addr.Port
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

	// Initialize the exit signal channel
	exitSignal = make(chan struct{})

	// Set up a signal channel to handle interrupts and termination signals
	go waitForSignal()

	// Start the background goroutine to periodically exchange CRDT data
	go exchangeCRDTData(cluster, counterCRDT)

	// Start the REPL for interacting with the Serf cluster
	go startREPL(config, cluster, counterCRDT)

	// Handle user and member events
	handleUserEvents(cluster, counterCRDT)
}

// parseFlags parses command-line flags and returns a ClusterConfig.
func parseFlags() ClusterConfig {
	var config ClusterConfig

	bindAddr := IPv4().String()
	bindPort := availablePort()
	bindPortStr := strconv.Itoa(bindPort)
	clusterPort := strconv.Itoa(bindPort)
	host := fmt.Sprintf("%v-%v", Hostname(), bindPortStr)

	flag.StringVar(&config.BindAddr, "bindAddr", bindAddr, "Address for the agent to listen for incoming connections")
	flag.StringVar(&config.BindPort, "bindPort", bindPortStr, "Port for the agent to listen for incoming connections")
	flag.StringVar(&config.ClusterAddr, "clusterAddr", bindAddr, "Address of the first agent in the cluster")
	flag.StringVar(&config.ClusterPort, "clusterPort", clusterPort, "Port of the first agent in the cluster")
	flag.StringVar(&config.Name, "name", host, "Unique name for the agent in the cluster")

	flag.Parse()

	return config
}

// waitForSignal sets up a signal channel to handle interrupts and termination signals.
func waitForSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}

// startREPL initiates the REPL for interacting with the Serf cluster.
func startREPL(config ClusterConfig, cluster *serf.Serf, crdt *CRDT) {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:       "> ",
		EOFPrompt:    "exit",
		HistoryFile:  "/tmp/readline.tmp",
		AutoComplete: &commandCompleter{},
	})
	if err != nil {
		fmt.Println("Error creating readline instance: ", err)
		return
	}
	defer rl.Close()

	fmt.Println("Welcome to the Serf Cluster REPL. Type 'help' for available commands.")

	for {
		line, err := rl.Readline()
		if err != nil {
			cluster.Leave()
			close(exitSignal)
			break
		}

		switch strings.TrimSpace(line) {
		case "help":
			fmt.Println("Available commands:")
			fmt.Println("  host      - List information about the host.")
			fmt.Println("  members   - List all servers connected to the cluster")
			fmt.Println("  crdt-edit - Increment the counter CRDT and distribute it to all nodes")
			fmt.Println("  crdt-show - Show the current value of the counter CRDT")
			fmt.Println("  exit      - Exit the REPL")
		case "host":
			listHost(config)
		case "members":
			listMembers(cluster)
		case "info":
			listInfo(config, cluster)
		case "crdt-edit":
			editCRDT(cluster, crdt)
		case "crdt-show":
			showCRDT(crdt)
		case "exit":
			fmt.Println("Exiting the REPL. Leaving the Serf cluster gracefully.")
			cluster.Leave()
			close(exitSignal)
			return
		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

// exchangeCRDTData periodically sends the CRDT value to other nodes in the cluster.
func exchangeCRDTData(cluster *serf.Serf, crdt *CRDT) {
	ticker := time.NewTicker(5 * time.Second) // Adjust the interval as needed
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Broadcast the current CRDT value to all nodes in the cluster
			broadcastUpdate(cluster, crdt)
		case <-exitSignal: // Check if exitSignal channel is closed
			fmt.Println("Exiting exchangeCRDTData goroutine.")
			return // Exit the goroutine
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
		case <-exitSignal: // Check if exitSignal channel is closed
			fmt.Println("Exiting handleUserEvents goroutine.")
			return // Exit the goroutine
		}
	}
}

// handleMemberUpdate processes member updates.
func handleMemberUpdate(event serf.Event, crdt *CRDT) {
	switch event.EventType() {
	case serf.EventMemberJoin:
		member := event.(serf.MemberEvent).Members[0]
		fmt.Printf("Member joined: %s\n", member.Name)
	case serf.EventMemberLeave:
		member := event.(serf.MemberEvent).Members[0]
		fmt.Printf("Member left: %s\n", member.Name)
	case serf.EventMemberFailed:
		member := event.(serf.MemberEvent).Members[0]
		fmt.Printf("Member failed: %s\n", member.Name)
	}
}

// handleUserEvent processes the received user event.
func handleUserEvent(event serf.UserEvent, crdt *CRDT) {
	if event.Name == "crdt-update" {
		// Decode the JSON payload into a map[string]int
		var data map[string]int
		if err := json.Unmarshal(event.Payload, &data); err != nil {
			fmt.Printf("Error decoding CRDT update payload: %v\n", err)
			return
		}

		// Extract clock and value from the decoded data
		clock := data["clock"]
		value := data["value"]

		// Update the CRDT with the decoded values
		if clock > crdt.clock {
			crdt.setValue(clock, value)
			fmt.Printf("Received CRDT update. New clock: %d, New value: %d\n", clock, value)
		}

	}
}

// marshalTags is a utility function which takes a map of tag key/value pairs
// and returns the same tags as strings in 'key=value' format.
func marshalTags(tags map[string]string) []string {
	var result []string
	for name, value := range tags {
		result = append(result, fmt.Sprintf("%s=%s", name, value))
	}
	return result
}

// unmarshalTags is a utility function which takes a slice of strings in
// key=value format and returns them as a tag mapping.
func unmarshalTags(tags []string) (map[string]string, error) {
	result := make(map[string]string)
	for _, tag := range tags {
		parts := strings.SplitN(tag, "=", 2)
		if len(parts) != 2 || len(parts[0]) == 0 {
			return nil, fmt.Errorf("invalid tag: '%s'", tag)
		}
		result[parts[0]] = parts[1]
	}
	return result, nil
}

// listInfo print host configuration and cluster memebers.
func listInfo(config ClusterConfig, cluster *serf.Serf) {
	listHost(config)
	listMembers(cluster)
}

// listInfo print host configuratio.
func listHost(config ClusterConfig) {
	hostTable := table.NewTableWriter("Host")
	hostTable.AddHeaders("Name", "Bind Address", "Bind Port", "Cluster Address", "Cluster Port")
	hostTable.AddRow(config.Name, config.BindAddr, config.BindPort, config.ClusterAddr, config.ClusterPort)
	fmt.Println(hostTable.Render())
}

// listMembers prints a list of all servers connected to the cluster.
func listMembers(cluster *serf.Serf) {
	membersTable := table.NewTableWriter("Members")
	membersTable.AddHeaders("Name", "Address", "Port")

	for _, m := range cluster.Members() {
		if m.Status != serf.StatusLeft {
			membersTable.AddRow(m.Name, m.Addr, m.Port)
		}
	}
	fmt.Println(membersTable.Render())
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

	// Marshal CRDT values to JSON
	data, err := json.Marshal(map[string]int{"clock": crdt.getClock(), "value": crdt.getValue()})
	if err != nil {
		fmt.Printf("Error marshaling CRDT data: %v\n", err)
		return
	}

	event := serf.UserEvent{
		Name:    "crdt-update",
		Payload: data,
	}

	err = cluster.UserEvent(event.Name, event.Payload, true)
	if err != nil {
		fmt.Printf("Error broadcasting CRDT update: %v\n", err)
	}
}

// CRDT methods
func (c *CRDT) increment() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock++
	c.value = c.value + 2
}

func (c *CRDT) setValue(clock int, value int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock = clock
	c.value = value
}

func (c *CRDT) getValue() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

func (c *CRDT) getClock() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clock
}

// commandCompleter is a custom completer struct for readline.
type commandCompleter struct{}

func (c *commandCompleter) Do(line []rune, pos int) (newLine [][]rune, length int) {
	// Discard anything after the cursor position.
	// This is similar behaviour to shell/bash.
	prefix := string(line[:pos])
	var suggestions [][]rune
	words := []string{"help", "host", "info", "members", "crdt-edit", "crdt-show", "exit"}

	// Simple hack to allow auto completion for help.
	if len(words) > 0 && words[0] == "help" {
		words = words[1:]
	}

	if len(prefix) > 0 {
		for _, cmd := range words {
			if strings.HasPrefix(cmd, prefix) {
				suggestions = append(suggestions, []rune(strings.TrimPrefix(cmd, prefix)))
			}
		}
	} else {
		for _, cmd := range words {
			suggestions = append(suggestions, []rune(cmd))
		}
	}

	// Append an empty space to each suggestions.
	for i, s := range suggestions {
		suggestions[i] = append(s, ' ')
	}

	return suggestions, len(prefix)
}
