package table

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

type tbl struct {
	writer table.Writer
}

var styles = map[string]table.Style{
	"":        table.StyleRounded,
	"rounded": table.StyleRounded,
	"double":  table.StyleDouble,
}

func NewTableWriter(format string, a ...any) *tbl {
	tbl := &tbl{
		writer: table.NewWriter(),
	}

	tbl.writer.SetStyle(styles["rounded"])

	tbl.writer.Style().Title.Align = text.AlignCenter
	tbl.writer.Style().Format.Header = text.FormatDefault
	tbl.writer.Style().Format.Footer = text.FormatDefault

	if format != "" {
		tbl.writer.SetTitle(fmt.Sprintf(format, a...))
	}

	return tbl
}

func (t *tbl) AddHeaders(items ...any) {
	t.writer.AppendHeader(items)
}

func (t *tbl) AddFooter(items ...any) {
	t.writer.AppendFooter(items)
}

func (t *tbl) AddSeparator() {
	t.writer.AppendSeparator()
}

func (t *tbl) AddRow(items ...any) {
	t.writer.AppendRow(items)
}

func (t *tbl) Render() string {
	return fmt.Sprintln(t.writer.Render())
}
