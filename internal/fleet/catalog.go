package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

// CatalogModel is one entry of `omp models --json`, the fields fleet reports.
type CatalogModel struct {
	Selector      string   `json:"selector"`
	ContextWindow int64    `json:"contextWindow"`
	Thinking      []string `json:"thinking"`
	Cost          struct {
		Input  float64 `json:"input"`
		Output float64 `json:"output"`
	} `json:"cost"`
}

// LoadCatalog asks the installed omp for its model catalog.
func LoadCatalog(ctx context.Context, home string) ([]CatalogModel, error) {
	out, err := exec.CommandContext(ctx, ompPath(home), "models", "--json").Output()
	if err != nil {
		return nil, fmt.Errorf("omp models --json: %w", err)
	}

	var parsed struct {
		Models []CatalogModel `json:"models"`
	}

	if err := json.Unmarshal(out, &parsed); err != nil {
		return nil, fmt.Errorf("parse omp catalog: %w", err)
	}

	return parsed.Models, nil
}

// Models renders the roster against the live catalog: tier, the thinking
// levels allowed (first is the default), the ladder the model supports, price per million tokens
// in and out, measured speed, context size and the operator's note. With
// all, every catalog model follows.
func Models(ctx context.Context, home string, all bool) (string, error) {
	roster, err := LoadRoster(home)
	if err != nil {
		return "", err
	}

	catalog, err := LoadCatalog(ctx, home)
	if err != nil {
		return "", err
	}

	return renderModels(catalog, roster, all), nil
}

type modelRow struct {
	selector, tier, thinking, ladder, price, tps, ctx, note string
}

func renderModels(catalog []CatalogModel, roster Roster, all bool) string {
	bySelector := make(map[string]CatalogModel, len(catalog))
	for _, m := range catalog {
		bySelector[m.Selector] = m
	}

	rows := make([]modelRow, 0, len(roster.Models))

	for selector, entry := range roster.Models {
		row := modelRow{selector: selector, tier: entry.Tier, thinking: entry.Thinking.String(), note: entry.Note}
		if entry.TPS > 0 {
			row.tps = fmt.Sprint(entry.TPS)
		}

		if m, ok := bySelector[selector]; ok {
			row.ladder, row.price, row.ctx = describe(m)
		} else {
			row.note = strings.TrimSpace("NOT IN OMP CATALOG; update omp. " + entry.Note)
		}

		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if ri, rj := tierRank(rows[i].tier), tierRank(rows[j].tier); ri != rj {
			return ri < rj
		}

		return rows[i].selector < rows[j].selector
	})

	if all {
		var rest []modelRow

		for _, m := range catalog {
			if _, listed := roster.Models[m.Selector]; listed || m.Selector == "" {
				continue
			}

			row := modelRow{selector: m.Selector}
			row.ladder, row.price, row.ctx = describe(m)
			rest = append(rest, row)
		}

		sort.Slice(rest, func(i, j int) bool { return rest[i].selector < rest[j].selector })
		rows = append(rows, rest...)
	}

	var b strings.Builder

	fmt.Fprintf(&b, "%-36s %-4s %-18s %-36s %-10s %-5s %-6s %s\n", "MODEL", "TIER", "THINK", "LADDER", "$IN/OUT/M", "TOK/S", "CTX", "NOTE")

	for _, r := range rows {
		fmt.Fprintf(&b, "%-36s %-4s %-18s %-36s %-10s %-5s %-6s %s\n", r.selector, r.tier, r.thinking, r.ladder, r.price, r.tps, r.ctx, r.note)
	}

	return b.String()
}

// tierRank orders S above A above B above C, and an untiered model last.
func tierRank(tier string) int {
	if i := strings.Index("SABC", tier); tier != "" && i >= 0 {
		return i
	}

	return len("SABC")
}

// describe renders the ladder, price and context of a catalog entry. A
// zero-priced model is billed against a subscription window, not dollars.
func describe(m CatalogModel) (ladder, price, ctx string) {
	ladder = strings.Join(m.Thinking, " ")
	if ladder == "" {
		ladder = "-"
	}

	price = "quota"
	if m.Cost.Input > 0 || m.Cost.Output > 0 {
		price = fmt.Sprintf("%g/%g", m.Cost.Input, m.Cost.Output)
	}

	return ladder, price, human(m.ContextWindow)
}
