// This file draws the SVG charts committed to results/ next to
// architecture.md: grouped bar charts and line charts, with an optional
// log scale (log10(1+v), so zero stays on the axis). Hovering a bar or a
// point shows its exact value. No dependencies beyond the standard library.

package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// palette is the categorical series order; series i always gets colour i.
var palette = []string{"#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#6250d6"}

const (
	chartW, chartH             = 720, 380
	padL, padR, padT, padB     = 70, 20, 64, 56
	ink, inkSoft, grid, surfce = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
)

// chart accumulates one SVG document.
type chart struct {
	b    strings.Builder
	logY bool
	yMax float64
}

// newChart starts a chart with its title, axis labels and a legend.
func newChart(title, xLabel, yLabel string, series []string, yMax float64, logY bool) *chart {
	c := &chart{logY: logY, yMax: yMax}
	fmt.Fprintf(&c.b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" font-family="system-ui,sans-serif" font-size="12">`, chartW, chartH)
	fmt.Fprintf(&c.b, `<rect width="100%%" height="100%%" fill="%s"/>`, surfce)
	fmt.Fprintf(&c.b, `<text x="%d" y="22" font-size="15" font-weight="600" fill="%s">%s</text>`, padL, ink, esc(title))
	x := padL
	for i, s := range series {
		fmt.Fprintf(&c.b, `<rect x="%d" y="34" width="12" height="12" rx="3" fill="%s"/><text x="%d" y="44" fill="%s">%s</text>`, x, palette[i], x+16, inkSoft, esc(s))
		x += 28 + 7*len(s)
	}
	fmt.Fprintf(&c.b, `<text x="%d" y="%d" text-anchor="middle" fill="%s">%s</text>`, padL+(chartW-padL-padR)/2, chartH-12, inkSoft, esc(xLabel))
	fmt.Fprintf(&c.b, `<text transform="translate(16,%d) rotate(-90)" text-anchor="middle" fill="%s">%s</text>`, padT+(chartH-padT-padB)/2, inkSoft, esc(yLabel))
	c.yAxis()
	return c
}

// y maps a value to its pixel row.
func (c *chart) y(v float64) float64 {
	f := v / c.yMax
	if c.logY {
		f = math.Log10(1+v) / math.Log10(1+c.yMax)
	}
	return float64(chartH-padB) - f*float64(chartH-padT-padB)
}

// yAxis draws horizontal grid lines with tick labels.
func (c *chart) yAxis() {
	var ticks []float64
	if c.logY {
		for t := 1.0; t <= c.yMax*1.0001; t *= 10 {
			ticks = append(ticks, t)
		}
		ticks = append([]float64{0}, ticks...)
	} else {
		step := niceStep(c.yMax / 5)
		for t := 0.0; t <= c.yMax*1.0001; t += step {
			ticks = append(ticks, t)
		}
	}
	for _, t := range ticks {
		y := c.y(t)
		fmt.Fprintf(&c.b, `<line x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke="%s"/>`, padL, chartW-padR, y, y, grid)
		fmt.Fprintf(&c.b, `<text x="%d" y="%.1f" text-anchor="end" fill="%s">%s</text>`, padL-6, y+4, inkSoft, num(t))
	}
}

// save closes the document and writes it to <out>/<name>.
func (c *chart) save(name string) {
	c.b.WriteString(`</svg>`)
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(*outDir, name), []byte(c.b.String()), 0o644); err != nil {
		panic(err)
	}
}

// barChart draws vals[series][group] as grouped bars, each labelled with
// its value, and saves it as <out>/<name>.
func barChart(name, title, xLabel, yLabel string, groups, series []string, vals [][]float64, logY bool) {
	c := newChart(title, xLabel, yLabel, series, maxOf(vals)*1.15, logY)
	gw := float64(chartW-padL-padR) / float64(len(groups))
	bw := math.Min(48, (gw-24)/float64(len(series)))
	for g, label := range groups {
		x0 := float64(padL) + gw*float64(g) + (gw-bw*float64(len(series)))/2
		fmt.Fprintf(&c.b, `<text x="%.1f" y="%d" text-anchor="middle" fill="%s">%s</text>`, float64(padL)+gw*(float64(g)+0.5), chartH-padB+18, ink, esc(label))
		for s := range series {
			v, x := vals[s][g], x0+bw*float64(s)
			y := c.y(v)
			fmt.Fprintf(&c.b, `<path d="M%.1f %d V%.1f a4 4 0 0 1 4 -4 H%.1f a4 4 0 0 1 4 4 V%d Z" fill="%s"><title>%s, %s: %s</title></path>`,
				x+1, chartH-padB, math.Min(y+4, float64(chartH-padB)), x+bw-5, chartH-padB, palette[s], esc(series[s]), esc(label), num(v))
			fmt.Fprintf(&c.b, `<text x="%.1f" y="%.1f" text-anchor="middle" font-size="11" fill="%s">%s</text>`, x+bw/2, y-4, inkSoft, num(v))
		}
	}
	c.save(name)
}

// lineChart draws ys[series][i] against xs[i] and saves it as <out>/<name>.
// Each line ends with a direct label naming its series.
func lineChart(name, title, xLabel, yLabel string, xs []float64, series []string, ys [][]float64, logY bool) {
	c := newChart(title, xLabel, yLabel, series, maxOf(ys)*1.15, logY)
	px := func(i int) float64 {
		return float64(padL+20) + float64(i)*float64(chartW-padL-padR-110)/float64(len(xs)-1)
	}
	for i, x := range xs {
		fmt.Fprintf(&c.b, `<text x="%.1f" y="%d" text-anchor="middle" fill="%s">%s</text>`, px(i), chartH-padB+18, ink, num(x))
	}
	for s := range series {
		var pts []string
		for i, v := range ys[s] {
			pts = append(pts, fmt.Sprintf("%.1f,%.1f", px(i), c.y(v)))
		}
		fmt.Fprintf(&c.b, `<polyline points="%s" fill="none" stroke="%s" stroke-width="2"/>`, strings.Join(pts, " "), palette[s])
		for i, v := range ys[s] {
			fmt.Fprintf(&c.b, `<circle cx="%.1f" cy="%.1f" r="4.5" fill="%s" stroke="%s" stroke-width="2"><title>%s at %s: %s</title></circle>`,
				px(i), c.y(v), palette[s], surfce, esc(series[s]), num(xs[i]), num(v))
		}
		last := len(xs) - 1
		fmt.Fprintf(&c.b, `<text x="%.1f" y="%.1f" fill="%s">%s</text>`, px(last)+10, c.y(ys[s][last])+4, ink, esc(series[s]))
	}
	c.save(name)
}

// maxOf returns the largest value in vals, at least 1.
func maxOf(vals [][]float64) float64 {
	m := 1.0
	for _, r := range vals {
		for _, v := range r {
			m = math.Max(m, v)
		}
	}
	return m
}

// niceStep rounds a raw tick step up to 1, 2 or 5 times a power of ten.
func niceStep(raw float64) float64 {
	p := math.Pow(10, math.Floor(math.Log10(raw)))
	for _, m := range []float64{1, 2, 5, 10} {
		if raw <= m*p {
			return m * p
		}
	}
	return 10 * p
}

// num formats a value compactly: 1234567 -> 1.2M, 12345 -> 12k, 0.25 -> 0.25.
func num(v float64) string {
	switch a := math.Abs(v); {
	case a >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case a >= 1e4:
		return fmt.Sprintf("%.0fk", v/1e3)
	case a >= 100 || v == math.Trunc(v):
		return fmt.Sprintf("%.0f", v)
	default:
		return fmt.Sprintf("%.2g", v)
	}
}

// esc escapes text for SVG.
func esc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}
