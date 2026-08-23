package mlow

import (
	"math"
	"sync"
)

// cpx is a single-precision complex value.
//
// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/674e85164b35ca19115dfebcf605708d15951ee7/wacore/src/voip/mlow/smpl_perc.rs#L318-L343
type cpx struct {
	re, im float32
}

func (a cpx) add(b cpx) cpx {
	return cpx{re: a.re + b.re, im: a.im + b.im}
}

func (a cpx) mul(b cpx) cpx {
	return cpx{
		re: a.re*b.re - a.im*b.im,
		im: a.re*b.im + a.im*b.re,
	}
}

// smallestFactor returns the smallest prime factor of n (>= 2).
func smallestFactor(n int) int {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/674e85164b35ca19115dfebcf605708d15951ee7/wacore/src/voip/mlow/smpl_perc.rs#L346-L358
	if n%2 == 0 {
		return 2
	}
	p := 3
	for p*p <= n {
		if n%p == 0 {
			return p
		}
		p += 2
	}
	return n
}

// twiddleKey identifies a precomputed twiddle table: its length and its direction.
type twiddleKey struct {
	n   int
	inv bool
}

// twiddleCache memoizes the twiddle tables. The encoder and the decoder only ever
// ask for the handful of transform lengths the codec is built around (512 for the
// LPC analysis, 576 for the perceptual model), so the cache holds a couple of
// entries for the life of the process and never grows with traffic.
var twiddleCache sync.Map // twiddleKey -> []cpx

// twiddleFactors returns the n-th roots of unity w[t] = e^(sign*i*2*pi*t/n),
// computed once per (length, direction) and shared afterwards.
//
// Every twiddle the mixed-radix recursion needs at a sub-length m that divides n
// is also an n-th root of unity, so one table of n entries serves every level:
// e^(sign*i*2*pi*k*q/m) == w[(k*q mod m)*(n/m)]. That is what keeps math.Cos and
// math.Sin out of the hot path — they used to run once per butterfly.
func twiddleFactors(n int, sign float32) []cpx {
	key := twiddleKey{n: n, inv: sign > 0}
	if v, ok := twiddleCache.Load(key); ok {
		return v.([]cpx)
	}
	w := make([]cpx, n)
	for t := 0; t < n; t++ {
		ang := float64(sign) * 2.0 * smplPI * float64(t) / float64(n)
		w[t] = cpx{re: float32(math.Cos(ang)), im: float32(math.Sin(ang))}
	}
	// LoadOrStore, not Store: two goroutines racing on the same length must end up
	// sharing one table instead of one of them replacing the other's in flight.
	actual, _ := twiddleCache.LoadOrStore(key, w)
	return actual.([]cpx)
}

// fftRec is the recursive mixed-radix Cooley-Tukey DFT. sign is -1 forward, +1
// inverse (unnormalized). x holds n inputs at the given stride; out is contiguous.
func fftRec(x []cpx, stride, n int, sign float32, out []cpx) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/674e85164b35ca19115dfebcf605708d15951ee7/wacore/src/voip/mlow/smpl_perc.rs#L362-L405
	if n == 1 {
		out[0] = x[0]
		return
	}
	fftRecTw(x, stride, n, out, twiddleFactors(n, sign), 1)
}

// fftRecTw is fftRec with the twiddle table threaded through. tw holds the N-th
// roots of unity of the top-level transform and step is N/n, so tw[t*step] is the
// t-th n-th root of unity at this level.
func fftRecTw(x []cpx, stride, n int, out []cpx, tw []cpx, step int) {
	if n == 1 {
		out[0] = x[0]
		return
	}
	p := smallestFactor(n)
	if p == n {
		for k := 0; k < n; k++ {
			var acc cpx
			for j := 0; j < n; j++ {
				acc = acc.add(x[j*stride].mul(tw[(k*j%n)*step]))
			}
			out[k] = acc
		}
		return
	}
	m := n / p
	sub := make([]cpx, n)
	for q := 0; q < p; q++ {
		fftRecTw(x[q*stride:], stride*p, m, sub[q*m:(q+1)*m], tw, step*p)
	}
	for k := 0; k < n; k++ {
		kmod := k % m
		var acc cpx
		for q := 0; q < p; q++ {
			acc = acc.add(sub[q*m+kmod].mul(tw[(k*q%n)*step]))
		}
		out[k] = acc
	}
}

// cfft computes the complex FFT of a mixed-radix length into out. sign=-1 forward,
// +1 inverse.
func cfft(input, out []cpx, sign float32) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/674e85164b35ca19115dfebcf605708d15951ee7/wacore/src/voip/mlow/smpl_perc.rs#L408-L412
	fftRec(input, 1, len(input), sign, out)
}

// rfftForwardOrdered is the forward real FFT of n real samples, re-packed into the
// ordered REAL layout: f[0]=DC.re, f[1]=Nyquist.re, then [re,im] pairs for bins
// 1..n/2-1. Output length is n.
func rfftForwardOrdered(time, f []float32) {
	// Source of truth: https://github.com/oxidezap/whatsapp-rust/blob/674e85164b35ca19115dfebcf605708d15951ee7/wacore/src/voip/mlow/smpl_perc.rs#L416-L432
	n := len(time)
	cin := make([]cpx, n)
	for i := 0; i < n; i++ {
		cin[i].re = time[i]
	}
	spec := make([]cpx, n)
	cfft(cin, spec, -1.0)
	f[0] = spec[0].re
	f[1] = spec[n/2].re
	for i := 1; i < n/2; i++ {
		f[2*i] = spec[i].re
		f[2*i+1] = spec[i].im
	}
}
