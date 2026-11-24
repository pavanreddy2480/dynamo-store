package scenario

import (
	"testing"
	"time"

	"dynamostore/store"
)

func TestPartitionScenarios(t *testing.T) {
	for _, o := range []ScenarioOptions{
		{Policy: store.KeepSiblings, R: 1, W: 1},
		{Policy: store.KeepSiblings, R: 2, W: 2},
		{Policy: store.KeepSiblings, R: 2, W: 2, Sloppy: true},
		{Policy: store.LastWriterWins, R: 1, W: 1},
		{Policy: store.LastWriterWins, R: 1, W: 1, SkewMinority: 50 * time.Millisecond},
	} {
		r := RunPartitionScenario(o)
		if !r.RuleHolds {
			t.Errorf("%+v: rule violated\n%v", o, r.Log)
		}
		if o.Policy == store.KeepSiblings && len(r.Siblings) != 2 {
			t.Errorf("%+v: want 2 siblings after heal, got %d", o, len(r.Siblings))
		}
		if o.Policy == store.KeepSiblings && !r.AckedKept {
			t.Errorf("%+v: acknowledged write lost", o)
		}
		if o.Policy == store.LastWriterWins && r.AckedKept {
			t.Errorf("%+v: expected LWW to drop one acknowledged write", o)
		}
	}
}
