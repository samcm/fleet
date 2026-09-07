package fleet

import (
	"os"
	"strings"
	"testing"
)

const accountsDoc = `accounts:
  work:
    provider: anthropic
    identity: email:sam@work.com|org:acme-1234
  personal:
    provider: anthropic
    identity: email:sam@home.com|org:solo-9
  research:
    provider: openai
    identity: email:sam@work.com|org:acme-1234
defaults:
  anthropic: work
`

// writeAccounts gives a home whose accounts file holds doc.
func writeAccounts(t *testing.T, doc string) string {
	t.Helper()

	home := t.TempDir()
	if err := os.MkdirAll(Root(home), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(accountsPath(home), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}

	return home
}

func TestLoadAccounts(t *testing.T) {
	home := writeAccounts(t, accountsDoc)

	c, err := loadAccounts(home)
	if err != nil {
		t.Fatal(err)
	}

	if len(c.Accounts) != 3 {
		t.Fatalf("loaded %d accounts, want 3", len(c.Accounts))
	}

	if work := c.Accounts["work"]; work.Provider != "anthropic" || work.Identity != "email:sam@work.com|org:acme-1234" {
		t.Fatalf("work loaded as %+v", work)
	}

	if research := c.Accounts["research"]; research.Provider != "openai" {
		t.Fatalf("research loaded as %+v", research)
	}

	if got := c.Defaults["anthropic"]; got != "work" {
		t.Fatalf("anthropic default %q, want work", got)
	}
}

func TestLoadAccountsWithoutFile(t *testing.T) {
	home := t.TempDir()

	c, err := loadAccounts(home)
	if err != nil {
		t.Fatalf("missing accounts file: %v", err)
	}

	if len(c.Accounts) != 0 || len(c.Defaults) != 0 {
		t.Fatalf("missing accounts file loaded %+v, want an empty config", c)
	}
}

func TestLoadAccountsRejectsInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
		want string
	}{
		{
			name: "accounts as a sequence",
			doc:  "accounts:\n  - work\n",
			want: "parse ",
		},
		{
			name: "account without a provider",
			doc:  "accounts:\n  work:\n    identity: email:sam@work.com|org:acme-1234\n",
			want: `account "work" has no provider`,
		},
		{
			name: "provider of only whitespace",
			doc:  "accounts:\n  work:\n    provider: \"   \"\n    identity: email:sam@work.com|org:acme-1234\n",
			want: `account "work" has no provider`,
		},
		{
			name: "identity without an organisation",
			doc:  "accounts:\n  work:\n    provider: anthropic\n    identity: email:sam@work.com\n",
			want: `account "work" identity "email:sam@work.com" is not email:<address>|org:<id>`,
		},
		{
			name: "identity without an address",
			doc:  "accounts:\n  work:\n    provider: anthropic\n    identity: email:|org:acme-1234\n",
			want: `account "work" identity "email:|org:acme-1234" is not email:<address>|org:<id>`,
		},
		{
			name: "identity with an empty organisation",
			doc:  "accounts:\n  work:\n    provider: anthropic\n    identity: \"email:sam@work.com|org:\"\n",
			want: `account "work" identity "email:sam@work.com|org:" is not email:<address>|org:<id>`,
		},
		{
			name: "identity halves the wrong way round",
			doc:  "accounts:\n  work:\n    provider: anthropic\n    identity: org:acme-1234|email:sam@work.com\n",
			want: `account "work" identity "org:acme-1234|email:sam@work.com" is not email:<address>|org:<id>`,
		},
		{
			name: "identity with a third component",
			doc:  "accounts:\n  work:\n    provider: anthropic\n    identity: email:sam@work.com|org:acme-1234|org:other\n",
			want: `account "work" identity "email:sam@work.com|org:acme-1234|org:other" is not email:<address>|org:<id>`,
		},
		{
			name: "identity components of only whitespace",
			doc:  "accounts:\n  work:\n    provider: anthropic\n    identity: \"email:  |org: \"\n",
			want: `account "work" identity "email:  |org: " is not email:<address>|org:<id>`,
		},
		{
			name: "account without an identity",
			doc:  "accounts:\n  work:\n    provider: anthropic\n",
			want: `account "work" identity "" is not email:<address>|org:<id>`,
		},
		{
			name: "default naming no configured account",
			doc:  accountsDoc + "  openai: nope\n",
			want: `default account "nope" for provider openai is not configured`,
		},
		{
			name: "default naming an account on another provider",
			doc:  accountsDoc + "  openai: work\n",
			want: `default account "work" for provider openai is for provider anthropic`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := writeAccounts(t, tc.doc)

			_, err := loadAccounts(home)
			if err == nil {
				t.Fatalf("loaded %q without error", tc.doc)
			}

			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want it to mention %q", err, tc.want)
			}

			if !strings.Contains(err.Error(), accountsPath(home)) {
				t.Fatalf("error %v omits the accounts file path", err)
			}
		})
	}
}

func TestAccountConfigResolve(t *testing.T) {
	home := writeAccounts(t, accountsDoc)
	path := accountsPath(home)

	c, err := loadAccounts(home)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name         string
		request      string
		model        string
		wantName     string
		wantIdentity string
		wantErr      string
	}{
		{
			name:         "named account on its own provider",
			request:      "personal",
			model:        "anthropic/claude-opus-5",
			wantName:     "personal",
			wantIdentity: "email:sam@home.com|org:solo-9",
		},
		{
			name:         "no request takes the provider default",
			request:      "",
			model:        "anthropic/claude-opus-5",
			wantName:     "work",
			wantIdentity: "email:sam@work.com|org:acme-1234",
		},
		{
			name:    "no request and no default pins nothing",
			request: "",
			model:   "openai/gpt-5",
		},
		{
			name:    "balanced pins nothing where a default exists",
			request: "balanced",
			model:   "anthropic/claude-opus-5",
		},
		{
			name:    "unknown account lists the configured names",
			request: "nope",
			model:   "anthropic/claude-opus-5",
			wantErr: `unknown account "nope" in ` + path + "; configured: personal, research, work",
		},
		{
			name:    "account belonging to another provider",
			request: "research",
			model:   "anthropic/claude-opus-5",
			wantErr: `account "research" is for provider openai; model anthropic/claude-opus-5 is on anthropic`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name, identity, err := c.resolve(tc.request, tc.model, path)

			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("resolve(%q, %q): %v", tc.request, tc.model, err)
			case tc.wantErr != "" && (err == nil || err.Error() != tc.wantErr):
				t.Fatalf("resolve(%q, %q) error %v, want %q", tc.request, tc.model, err, tc.wantErr)
			}

			if name != tc.wantName || identity != tc.wantIdentity {
				t.Fatalf("resolve(%q, %q) = %q, %q; want %q, %q", tc.request, tc.model, name, identity, tc.wantName, tc.wantIdentity)
			}
		})
	}
}

func TestAccountConfigResolveWithoutAccounts(t *testing.T) {
	home := t.TempDir()
	path := accountsPath(home)

	c, err := loadAccounts(home)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = c.resolve("work", "anthropic/claude-opus-5", path)
	if err == nil || err.Error() != "no accounts configured at "+path {
		t.Fatalf("resolve of a named account with no config: %v", err)
	}

	for _, request := range []string{"", accountBalanced} {
		name, identity, err := c.resolve(request, "anthropic/claude-opus-5", path)
		if err != nil || name != "" || identity != "" {
			t.Fatalf("resolve(%q) with no config = %q, %q, %v; want no pin", request, name, identity, err)
		}
	}
}
