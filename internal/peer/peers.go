package peer

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
)

// Peers holds the collection of peers for the cluster
type Peers interface {
	GetPeers() ([]string, error)
	GetInstanceID() (string, error)
	RegisterUpdatedPeersCallback(callback func())
	Ready() error
	// make it injectable
	startstop.Starter
}

func publicAddr(logger logger.Logger, cfg config.Config) (string, error) {
	// compute the public version of my peer listen address
	listenAddr := cfg.GetPeerListenAddr()
	// first, extract the port
	_, port, err := net.SplitHostPort(listenAddr)

	if err != nil {
		return "", err
	}

	var myIdentifier string

	// If RedisIdentifier is set, use as identifier.
	if redisIdentifier := cfg.GetRedisIdentifier(); redisIdentifier != "" {
		myIdentifier = redisIdentifier
		logger.Info().WithField("identifier", myIdentifier).Logf("using specified RedisIdentifier from config")
	} else {
		// Otherwise, determine identifier from network interface.
		myIdentifier, err = getIdentifierFromInterface(logger, cfg)
		if err != nil {
			return "", err
		}
	}

	publicListenAddr := fmt.Sprintf("http://%s:%s", myIdentifier, port)

	return publicListenAddr, nil
}

// getIdentifierFromInterface returns a string that uniquely identifies this
// host in the network. If an interface is specified, it will scan it to
// determine an identifier from the first IP address on that interface.
// Otherwise, it will use the hostname.
func getIdentifierFromInterface(logger logger.Logger, cfg config.Config) (string, error) {
	myIdentifier, _ := os.Hostname()
	identifierInterfaceName := cfg.GetIdentifierInterfaceName()

	if identifierInterfaceName != "" {
		ifc, err := net.InterfaceByName(identifierInterfaceName)
		if err != nil {
			logger.Error().WithField("interface", identifierInterfaceName).
				Logf("IdentifierInterfaceName set but couldn't find interface by that name")
			return "", err
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			logger.Error().WithField("interface", identifierInterfaceName).
				Logf("IdentifierInterfaceName set but couldn't list addresses")
			return "", err
		}
		var ipStr string
		for _, addr := range addrs {
			// ParseIP doesn't know what to do with the suffix
			ip := net.ParseIP(strings.Split(addr.String(), "/")[0])
			ipv6 := cfg.GetUseIPV6Identifier()
			if ipv6 && ip.To16() != nil {
				ipStr = fmt.Sprintf("[%s]", ip.String())
				break
			}
			if !ipv6 && ip.To4() != nil {
				ipStr = ip.String()
				break
			}
		}
		if ipStr == "" {
			err = errors.New("could not find a valid IP to use from interface")
			logger.Error().WithField("interface", ifc.Name).
				Logf("IdentifierInterfaceName set but couldn't find a valid IP to use from interface")
			return "", err
		}
		myIdentifier = ipStr
		logger.Info().WithField("identifier", myIdentifier).WithField("interface", ifc.Name).
			Logf("using identifier from interface")
	}

	return myIdentifier, nil
}
