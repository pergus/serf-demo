package main

/*
./serf -bindAddr 127.0.0.1 -bindPort 6666 -advertiseAddr 127.0.0.1 -advertisePort 6666 -clusterAddr 127.0.0.1 -clusterPort 6666 -name a1
./serf -bindAddr 127.0.0.1 -bindPort 7777 -advertiseAddr 127.0.0.1 -advertisePort 7777 -clusterAddr 127.0.0.1 -clusterPort 6666 -name a2
*/

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/hashicorp/serf/serf"
	"github.com/pkg/errors"
)

// setupCluster initializes and joins a Serf cluster.
func setupCluster(bindAddr, bindPort, advertiseAddr, advertisePort, clusterAddr, clusterPort, name string) (*serf.Serf, error) {
	// Initialize Serf configuration
	conf := serf.DefaultConfig()
	conf.Init()

	// Set Memberlist configuration
	conf.MemberlistConfig.AdvertiseAddr = advertiseAddr
	conf.MemberlistConfig.AdvertisePort, _ = strconv.Atoi(advertisePort)
	conf.MemberlistConfig.BindAddr = bindAddr
	conf.MemberlistConfig.BindPort, _ = strconv.Atoi(bindPort)
	conf.MemberlistConfig.ProtocolVersion = 3 // Version 3 enables binding different ports for each agent
	conf.NodeName = name

	// Create a Serf cluster
	cluster, err := serf.Create(conf)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create cluster")
	}

	// Join the cluster with the specified address and port
	_, err = cluster.Join([]string{clusterAddr + ":" + clusterPort}, true)
	if err != nil {
		log.Printf("Couldn't join cluster, starting own: %v\n", err)
	}

	return cluster, nil
}

// main function
func main() {
	// Define command-line flags for addresses and ports
	bindAddr := flag.String("bindAddr", "127.0.0.1", "Address for the agent to listen for incoming connections")
	bindPort := flag.String("bindPort", "6666", "Port for the agent to listen for incoming connections")
	advertiseAddr := flag.String("advertiseAddr", "127.0.0.1", "Address for the agent to be reachable")
	advertisePort := flag.String("advertisePort", "6666", "Port for the agent to be reachable")
	clusterAddr := flag.String("clusterAddr", "127.0.0.1", "Address of the first agent in the cluster")
	clusterPort := flag.String("clusterPort", "6666", "Port of the first agent in the cluster")
	name := flag.String("name", "default", "Unique name for the agent in the cluster")
	flag.Parse()

	// Set up the Serf cluster
	cluster, err := setupCluster(
		*bindAddr, *bindPort,
		*advertiseAddr, *advertisePort,
		*clusterAddr, *clusterPort,
		*name,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Set up a signal channel to handle interrupts and termination signals
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	// Leave the Serf cluster gracefully
	cluster.Leave()
	os.Exit(1)
}

