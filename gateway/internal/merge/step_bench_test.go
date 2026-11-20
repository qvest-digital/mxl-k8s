package merge

import (
	"testing"
	"time"
)

// The cost of Audio.Step per page of 1024 samples for three members of
// 64 channels each, whose samples are zero on every channel at 30% of
// positions, singly: low-level audio, where every zero is a sample to
// weigh the members at. With gaps, member 0, the primary, also holds a
// 16-sample gap every 300 samples that the others do not, which the
// output takes from another member.
func BenchmarkAudioStepDenseZeros(b *testing.B) {
	const members, pages, channels = 3, 38, 64
	length := uint64(pages * PageSamples)
	dense := func(i uint64) bool { return (i%length)*2654435761%10 < 3 }
	for _, tc := range []struct {
		name string
		gap  func(i uint64) bool
	}{
		{"identical", func(uint64) bool { return false }},
		{"gaps", func(i uint64) bool { return i%length%300 < 16 }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			a := NewAudio(members, pages, members)
			a.Skew, a.Tolerance = 2048, ToleranceFor(40*time.Millisecond)
			s := newSamples(members, channels, pages)
			setZeros(a, s)
			for m := range members {
				s.write(m, 0, length, func(i uint64) float32 {
					if dense(i) || (m == 0 && tc.gap(i)) {
						return 0
					}
					return content(i % length)
				})
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
		})
	}
}

func setZeros(a *Audio, s *samples) { a.Zeros, a.Content = s.zeros, s.content }
