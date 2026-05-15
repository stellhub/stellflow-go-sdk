package transport

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

const DefaultPort = 9092

// Endpoint identifies one Stellflow broker data-plane address.
type Endpoint struct {
	Host string
	Port int
}

// ParseEndpoint parses host:port or stellflow://host:port.
func ParseEndpoint(value string) (Endpoint, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return Endpoint{}, fmt.Errorf("endpoint must not be blank")
	}
	if strings.Contains(value, "://") {
		parsed, err := url.Parse(value)
		if err != nil {
			return Endpoint{}, err
		}
		if parsed.Scheme != "stellflow" {
			return Endpoint{}, fmt.Errorf("unsupported endpoint scheme: %s", parsed.Scheme)
		}
		value = parsed.Host
	}
	host, portValue, err := net.SplitHostPort(value)
	if err != nil {
		if strings.Contains(err.Error(), "missing port in address") {
			return Endpoint{Host: value, Port: DefaultPort}, nil
		}
		return Endpoint{}, err
	}
	port, err := strconv.Atoi(portValue)
	if err != nil {
		return Endpoint{}, err
	}
	if host == "" || port <= 0 {
		return Endpoint{}, fmt.Errorf("invalid endpoint: %s", value)
	}
	return Endpoint{Host: host, Port: port}, nil
}

// Address returns host:port for net.Dial.
func (e Endpoint) Address() string {
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}

func (e Endpoint) String() string {
	return "stellflow://" + e.Address()
}
