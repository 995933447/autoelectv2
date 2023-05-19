package util

import (
	"net"
)

func GetMacAddrs() ([]string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var macAddrs []string
	for _, netInterface := range interfaces {
		if macAddr := netInterface.HardwareAddr.String(); macAddr != "" {
			macAddrs = append(macAddrs, macAddr)
		}
	}
	return macAddrs, nil
}

func GetLocalIpsWithoutLoopback() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	var ips []string
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && IsLocalIpV4(ipNet.IP) {
			ips = append(ips, ipNet.IP.String())
		}
	}
	return ips, nil
}

func IsLocalIpV4(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip4 := ip.To4(); ip4 != nil {
		switch true {
		case ip4[0] == 10:
			return true
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			return true
		case ip4[0] == 192 && ip4[1] == 168:
			return true
		}
	}
	return false
}
