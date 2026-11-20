package merge

import (
	"testing"
	"time"
)

// The cost of Audio.Step per page of 1024 samples for three members of
// 64 channels each, whose samples are zero on every channel at 30% of
// positions, singly: low-level audio, where every zero is a sample to
// weigh the members at.
func BenchmarkAudioStepDenseZeros(b *testing.B) {
	const members, pages, channels = 3, 38, 64
	a := NewAudio(members, pages, members)
	a.Skew, a.Tolerance = 2048, ToleranceFor(40*time.Millisecond)
	s := newSamples(members, channels, pages)
	setZeros(a, s)
	length := uint64(pages * PageSamples)
	dense := func(i uint64) float32 {
		if (i%length)*2654435761%10 < 3 {
			return 0
		}
		return content(i % length)
	}
	for m := range members {
		s.write(m, 0, length, dense)
	}
	now := time.Unix(0, 0)
	from := uint64(PageSamples)
	for b.Loop() {
		to := from + PageSamples
		for m := range members {
			a.Committed(m, from, to)
		}
		now = now.Add(PageSamples * time.Second / 48000)
		if _, head, _ := a.Step(now); head != to {
			b.Fatalf("head %d, want %d", head, to)
		}
		from = to
	}
}

func setZeros(a *Audio, s *samples) { a.Run, a.Zeros, a.Content = 48, s.zeros, s.content }
