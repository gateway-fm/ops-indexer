package indexer

import (
	"context"
	"strings"
	"sync"
)

type TokenCache struct {
	mu     sync.RWMutex
	tokens map[string]struct{}
}

func NewTokenCache() *TokenCache {
	return &TokenCache{
		tokens: make(map[string]struct{}),
	}
}

func (c *TokenCache) Has(address string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.tokens[strings.ToLower(address)]
	return exists
}

func (c *TokenCache) Add(address string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tokens[strings.ToLower(address)] = struct{}{}
}

func (c *TokenCache) AddBatch(addresses []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, addr := range addresses {
		c.tokens[strings.ToLower(addr)] = struct{}{}
	}
}

func (c *TokenCache) LoadFromDB(ctx context.Context, database Database) error {
	addresses, err := database.GetAllTokenAddresses(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, addr := range addresses {
		c.tokens[strings.ToLower(addr)] = struct{}{}
	}
	return nil
}

func (c *TokenCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.tokens)
}

type ContractCache struct {
	mu        sync.RWMutex
	contracts map[string]struct{}
}

func NewContractCache() *ContractCache {
	return &ContractCache{
		contracts: make(map[string]struct{}),
	}
}

func (c *ContractCache) Has(address string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.contracts[strings.ToLower(address)]
	return exists
}

func (c *ContractCache) Add(address string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.contracts[strings.ToLower(address)] = struct{}{}
}

func (c *ContractCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.contracts)
}
