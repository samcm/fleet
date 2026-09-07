package fleet

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// accountBalanced is the account a spawn names to turn the pin off, leaving the
// worker on whichever identity the provider balances it onto.
const accountBalanced = "balanced"

type accountEntry struct {
	Provider string `yaml:"provider"`
	Identity string `yaml:"identity"`
}

// accountConfig is the operator's OAuth accounts keyed by name, plus the
// account each provider pins to when a spawn names none. It is one file so the
// CLI and the MCP tools pin the same way.
type accountConfig struct {
	Accounts map[string]accountEntry `yaml:"accounts"`
	Defaults map[string]string       `yaml:"defaults"`
}

func accountsPath(home string) string { return filepath.Join(Root(home), "accounts.yaml") }

// loadAccounts reads the accounts file; a missing file is an empty config,
// which pins nothing.
func loadAccounts(home string) (accountConfig, error) {
	var c accountConfig

	path := accountsPath(home)

	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}

	if err != nil {
		return c, fmt.Errorf("read accounts: %w", err)
	}

	if err := yaml.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("parse %s: %w", path, err)
	}

	return c, c.validate(path)
}

// names lists the account names in sorted order, so what an operator who named
// an account wrong reads back is stable.
func (c accountConfig) names() []string {
	return slices.Sorted(maps.Keys(c.Accounts))
}

// validate reports the first fault in name order: an account needs a provider
// and an identity fleet can pin to, and a provider's default must name a
// configured account on that same provider.
func (c accountConfig) validate(path string) error {
	for _, name := range c.names() {
		entry := c.Accounts[name]

		switch {
		case strings.TrimSpace(entry.Provider) == "":
			return fmt.Errorf("%s: account %q has no provider", path, name)
		case !validIdentity(entry.Identity):
			return fmt.Errorf("%s: account %q identity %q is not email:<address>|org:<id>", path, name, entry.Identity)
		}
	}

	for _, provider := range slices.Sorted(maps.Keys(c.Defaults)) {
		name := c.Defaults[provider]

		entry, ok := c.Accounts[name]
		if !ok {
			return fmt.Errorf("%s: default account %q for provider %s is not configured", path, name, provider)
		}

		if entry.Provider != provider {
			return fmt.Errorf("%s: default account %q for provider %s is for provider %s", path, name, provider, entry.Provider)
		}
	}

	return nil
}

// resolve returns the applied name and identity; an empty request uses the
// provider default, while balanced explicitly opts out.
func (c accountConfig) resolve(name, model, path string) (string, string, error) {
	if name == accountBalanced {
		return "", "", nil
	}

	provider, _, _ := strings.Cut(model, "/")

	if name == "" {
		name = c.Defaults[provider]
		if name == "" {
			return "", "", nil
		}
	}

	entry, ok := c.Accounts[name]
	if !ok {
		if len(c.Accounts) == 0 {
			return "", "", fmt.Errorf("no accounts configured at %s", path)
		}

		return "", "", fmt.Errorf("unknown account %q in %s; configured: %s", name, path, strings.Join(c.names(), ", "))
	}

	if entry.Provider != provider {
		return "", "", fmt.Errorf("account %q is for provider %s; model %s is on %s", name, entry.Provider, model, provider)
	}

	return name, entry.Identity, nil
}

// validIdentity reports whether an identity names both halves of an OAuth
// account, and nothing else: email:<address>|org:<id>.
func validIdentity(identity string) bool {
	email, org, ok := strings.Cut(identity, "|")
	if !ok || strings.Contains(org, "|") {
		return false
	}

	address, ok := strings.CutPrefix(email, "email:")
	if !ok || strings.TrimSpace(address) == "" {
		return false
	}

	id, ok := strings.CutPrefix(org, "org:")

	return ok && strings.TrimSpace(id) != ""
}
