package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	blockHash := c.Hash(blockId)
	return c.ServerMap[blockHash]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// hash servers on hash ring
	var c ConsistentHashRing
	serverMap := make(map[string]string)	// hash: serverName
	for _, serverName :=  range serverAddrs {
		serverHash := c.Hash(serverName);
		serverMap[serverHash] = serverName
	}

	c.ServerMap = serverMap
	return &c
}
