package main

import "testing"

func TestContinuityCarriesStatusAcrossMissingDays(t *testing.T) {
	for _, duty := range []string{"OFF", "SB", "ON", "DR"} {
		got := continuousDay(nil, SegmentResponse{DutyStatus: duty, Origin: "System"}, 86400, false)
		if len(got) != 1 || got[0].DutyStatus != duty || got[0].StartSecond != 0 || *got[0].EndSecond != 86400 {
			t.Fatalf("%s: %#v", duty, got)
		}
	}
}
func TestContinuityFillsBeginningAndExtendsLastStatus(t *testing.T) {
	old := []SegmentResponse{{ID: "existing", DutyStatus: "DR", StartMinute: 800, StartSecond: 48000, EndMinute: intPointer(1020), EndSecond: intPointer(61200)}}
	got := continuousDay(old, SegmentResponse{DutyStatus: "SB"}, 86400, false)
	if len(got) != 2 || got[0].DutyStatus != "SB" || *got[0].EndSecond != 48000 || got[1].ID != "existing" || *got[1].EndSecond != 86400 {
		t.Fatalf("%#v", got)
	}
}
func TestTodayRemainsOpen(t *testing.T) {
	got := continuousDay(nil, SegmentResponse{DutyStatus: "ON"}, 43200, true)
	if len(got) != 1 || got[0].EndMinute != nil || got[0].EndSecond != nil {
		t.Fatalf("%#v", got)
	}
}
func TestCrossDateSwapPreservesIntermediateDurations(t *testing.T) {
	input := []swapBlock{{Segment: SegmentResponse{DutyStatus: "SB"}, Duration: 36000}, {Segment: SegmentResponse{DutyStatus: "DR"}, Duration: 10000}, {Segment: SegmentResponse{DutyStatus: "OFF"}, Duration: 40000}, {Segment: SegmentResponse{DutyStatus: "ON"}, Duration: 18000}}
	out, err := retimeSwap(input, 0, 3, 18000)
	if err != nil {
		t.Fatal(err)
	}
	totalA, totalB := 0, 0
	for i, x := range out {
		totalA += input[i].Duration
		totalB += x.Duration
		if x.Segment.DutyStatus != input[i].Segment.DutyStatus {
			t.Fatal("status order changed")
		}
	}
	if totalA != totalB || out[1].Duration != 10000 || out[2].Duration != 40000 || out[3].Duration != 36000 {
		t.Fatalf("%#v", out)
	}
	reverse, err := retimeSwap(input, 3, 0, 36000)
	if err != nil || reverse[0].Duration != 18000 {
		t.Fatal("reverse selection failed")
	}
}
func TestSwapRejectsInvalidSelections(t *testing.T) {
	in := []swapBlock{{Duration: 10}, {Duration: 20}}
	for _, args := range [][3]int{{0, 0, 3}, {-1, 1, 3}, {0, 1, 0}, {0, 1, 30}} {
		if _, e := retimeSwap(in, args[0], args[1], args[2]); e == nil {
			t.Fatalf("accepted %#v", args)
		}
	}
}
