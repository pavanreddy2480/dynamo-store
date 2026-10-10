package vclock

import "testing"

func TestCompare(t *testing.T) {
	a := VC{"n1": 1}
	b := a.Increment("n1", 0)           // n1:2
	c := a.Increment("n2", 0)           // n1:1 n2:1
	d := Merge(b, c).Increment("n3", 0) // reconciles b and c

	cases := []struct {
		x, y VC
		want Order
	}{
		{a, a, Equal},
		{a, b, Before},
		{b, a, After},
		{b, c, Concurrent},
		{c, b, Concurrent},
		{b, d, Before},
		{c, d, Before},
		{New(), a, Before},
		{VC{"n1": 0}, New(), Equal},
	}
	for i, tc := range cases {
		if got := Compare(tc.x, tc.y); got != tc.want {
			t.Errorf("case %d: Compare(%v,%v)=%v want %v", i, tc.x, tc.y, got, tc.want)
		}
	}
}

func TestIncrementFloor(t *testing.T) {
	v := VC{"n1": 1}.Increment("n1", 5)
	if v["n1"] != 6 {
		t.Fatalf("got %v", v)
	}
}
