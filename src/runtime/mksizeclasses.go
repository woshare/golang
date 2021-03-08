// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build ignore
// +build ignore

// Generate tables for small malloc size classes.
//
// See malloc.go for overview.
//
// The size classes are chosen so that rounding an allocation
// request up to the next size class wastes at most 12.5% (1.125x).
//
// Each size class has its own page count that gets allocated
// and chopped up when new objects of the size class are needed.
// That page count is chosen so that chopping up the run of
// pages into objects of the given size wastes at most 12.5% (1.125x)
// of the memory. It is not necessary that the cutoff here be
// the same as above.
//
// The two sources of waste multiply, so the worst possible case
// for the above constraints would be that allocations of some
// size might have a 26.6% (1.266x) overhead.
// In practice, only one of the wastes comes into play for a
// given size (sizes < 512 waste mainly on the round-up,
// sizes > 512 waste mainly on the page chopping).
// For really small sizes, alignment constraints force the
// overhead higher.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"io"
	"log"
	"math/bits"
	"os"
)

// Generate msize.go

var stdout = flag.Bool("stdout", false, "write to stdout instead of sizeclasses.go")

func main() {
	flag.Parse()

	var b bytes.Buffer
	fmt.Fprintln(&b, "// Code generated by mksizeclasses.go; DO NOT EDIT.")
	fmt.Fprintln(&b, "//go:generate go run mksizeclasses.go")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "package runtime")
	classes := makeClasses()

	printComment(&b, classes)

	printClasses(&b, classes)

	out, err := format.Source(b.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	if *stdout {
		_, err = os.Stdout.Write(out)
	} else {
		err = os.WriteFile("sizeclasses.go", out, 0666)
	}
	if err != nil {
		log.Fatal(err)
	}
}

const (
	// Constants that we use and will transfer to the runtime.
	maxSmallSize = 32 << 10
	smallSizeDiv = 8
	smallSizeMax = 1024
	largeSizeDiv = 128
	pageShift    = 13

	// Derived constants.
	pageSize = 1 << pageShift
)

type class struct {
	size   int // max size
	npages int // number of pages

	mul    int
	shift  uint
	shift2 uint
	mask   int
}

func powerOfTwo(x int) bool {
	return x != 0 && x&(x-1) == 0
}

func makeClasses() []class {
	var classes []class

	classes = append(classes, class{}) // class #0 is a dummy entry

	align := 8
	for size := align; size <= maxSmallSize; size += align {
		if powerOfTwo(size) { // bump alignment once in a while
			if size >= 2048 {
				align = 256
			} else if size >= 128 {
				align = size / 8
			} else if size >= 32 {
				align = 16 // heap bitmaps assume 16 byte alignment for allocations >= 32 bytes.
			}
		}
		if !powerOfTwo(align) {
			panic("incorrect alignment")
		}

		// Make the allocnpages big enough that
		// the leftover is less than 1/8 of the total,
		// so wasted space is at most 12.5%.
		allocsize := pageSize
		for allocsize%size > allocsize/8 {
			allocsize += pageSize
		}
		npages := allocsize / pageSize

		// If the previous sizeclass chose the same
		// allocation size and fit the same number of
		// objects into the page, we might as well
		// use just this size instead of having two
		// different sizes.
		if len(classes) > 1 && npages == classes[len(classes)-1].npages && allocsize/size == allocsize/classes[len(classes)-1].size {
			classes[len(classes)-1].size = size
			continue
		}
		classes = append(classes, class{size: size, npages: npages})
	}

	// Increase object sizes if we can fit the same number of larger objects
	// into the same number of pages. For example, we choose size 8448 above
	// with 6 objects in 7 pages. But we can well use object size 9472,
	// which is also 6 objects in 7 pages but +1024 bytes (+12.12%).
	// We need to preserve at least largeSizeDiv alignment otherwise
	// sizeToClass won't work.
	for i := range classes {
		if i == 0 {
			continue
		}
		c := &classes[i]
		psize := c.npages * pageSize
		new_size := (psize / (psize / c.size)) &^ (largeSizeDiv - 1)
		if new_size > c.size {
			c.size = new_size
		}
	}

	if len(classes) != 68 {
		panic("number of size classes has changed")
	}

	for i := range classes {
		computeDivMagic(&classes[i])
	}

	return classes
}

// computeDivMagic computes some magic constants to implement
// the division required to compute object number from span offset.
// n / c.size is implemented as n >> c.shift * c.mul >> c.shift2
// for all 0 <= n <= c.npages * pageSize
func computeDivMagic(c *class) {
	// divisor
	d := c.size
	if d == 0 {
		return
	}

	// maximum input value for which the formula needs to work.
	max := c.npages * pageSize

	if powerOfTwo(d) {
		// If the size is a power of two, heapBitsForObject can divide even faster by masking.
		// Compute this mask.
		if max >= 1<<16 {
			panic("max too big for power of two size")
		}
		c.mask = 1<<16 - d
	}

	// Compute pre-shift by factoring power of 2 out of d.
	for d%2 == 0 {
		c.shift++
		d >>= 1
		max >>= 1
	}

	// Find the smallest k that works.
	// A small k allows us to fit the math required into 32 bits
	// so we can use 32-bit multiplies and shifts on 32-bit platforms.
nextk:
	for k := uint(0); ; k++ {
		mul := (int(1)<<k + d - 1) / d //  ⌈2^k / d⌉

		// Test to see if mul works.
		for n := 0; n <= max; n++ {
			if n*mul>>k != n/d {
				continue nextk
			}
		}
		if mul >= 1<<16 {
			panic("mul too big")
		}
		if uint64(mul)*uint64(max) >= 1<<32 {
			panic("mul*max too big")
		}
		c.mul = mul
		c.shift2 = k
		break
	}

	// double-check.
	for n := 0; n <= max; n++ {
		if n*c.mul>>c.shift2 != n/d {
			fmt.Printf("d=%d max=%d mul=%d shift2=%d n=%d\n", d, max, c.mul, c.shift2, n)
			panic("bad multiply magic")
		}
		// Also check the exact computations that will be done by the runtime,
		// for both 32 and 64 bit operations.
		if uint32(n)*uint32(c.mul)>>uint8(c.shift2) != uint32(n/d) {
			fmt.Printf("d=%d max=%d mul=%d shift2=%d n=%d\n", d, max, c.mul, c.shift2, n)
			panic("bad 32-bit multiply magic")
		}
		if uint64(n)*uint64(c.mul)>>uint8(c.shift2) != uint64(n/d) {
			fmt.Printf("d=%d max=%d mul=%d shift2=%d n=%d\n", d, max, c.mul, c.shift2, n)
			panic("bad 64-bit multiply magic")
		}
	}
}

func printComment(w io.Writer, classes []class) {
	fmt.Fprintf(w, "// %-5s  %-9s  %-10s  %-7s  %-10s  %-9s  %-9s\n", "class", "bytes/obj", "bytes/span", "objects", "tail waste", "max waste", "min align")
	prevSize := 0
	var minAligns [32]int
	for i, c := range classes {
		if i == 0 {
			continue
		}
		spanSize := c.npages * pageSize
		objects := spanSize / c.size
		tailWaste := spanSize - c.size*(spanSize/c.size)
		maxWaste := float64((c.size-prevSize-1)*objects+tailWaste) / float64(spanSize)
		alignBits := bits.TrailingZeros(uint(c.size))
		for i := range minAligns {
			if i > alignBits {
				minAligns[i] = 0
			} else if minAligns[i] == 0 {
				minAligns[i] = c.size
			}
		}
		prevSize = c.size
		fmt.Fprintf(w, "// %5d  %9d  %10d  %7d  %10d  %8.2f%%  %9d\n", i, c.size, spanSize, objects, tailWaste, 100*maxWaste, 1<<alignBits)
	}
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "// %-9s  %-4s  %-12s\n", "alignment", "bits", "min obj size")
	for bits, size := range minAligns {
		if size == 0 {
			break
		}
		if bits+1 < len(minAligns) && size == minAligns[bits+1] {
			continue
		}
		fmt.Fprintf(w, "// %9d  %4d  %12d\n", 1<<bits, bits, size)
	}
	fmt.Fprintf(w, "\n")
}

func printClasses(w io.Writer, classes []class) {
	fmt.Fprintln(w, "const (")
	fmt.Fprintf(w, "_MaxSmallSize = %d\n", maxSmallSize)
	fmt.Fprintf(w, "smallSizeDiv = %d\n", smallSizeDiv)
	fmt.Fprintf(w, "smallSizeMax = %d\n", smallSizeMax)
	fmt.Fprintf(w, "largeSizeDiv = %d\n", largeSizeDiv)
	fmt.Fprintf(w, "_NumSizeClasses = %d\n", len(classes))
	fmt.Fprintf(w, "_PageShift = %d\n", pageShift)
	fmt.Fprintln(w, ")")

	fmt.Fprint(w, "var class_to_size = [_NumSizeClasses]uint16 {")
	for _, c := range classes {
		fmt.Fprintf(w, "%d,", c.size)
	}
	fmt.Fprintln(w, "}")

	fmt.Fprint(w, "var class_to_allocnpages = [_NumSizeClasses]uint8 {")
	for _, c := range classes {
		fmt.Fprintf(w, "%d,", c.npages)
	}
	fmt.Fprintln(w, "}")

	fmt.Fprintln(w, "type divMagic struct {")
	fmt.Fprintln(w, "  shift uint8")
	fmt.Fprintln(w, "  shift2 uint8")
	fmt.Fprintln(w, "  mul uint16")
	fmt.Fprintln(w, "  baseMask uint16")
	fmt.Fprintln(w, "}")
	fmt.Fprint(w, "var class_to_divmagic = [_NumSizeClasses]divMagic {")
	for _, c := range classes {
		fmt.Fprintf(w, "{%d,%d,%d,%d},", c.shift, c.shift2, c.mul, c.mask)
	}
	fmt.Fprintln(w, "}")

	// map from size to size class, for small sizes.
	sc := make([]int, smallSizeMax/smallSizeDiv+1)
	for i := range sc {
		size := i * smallSizeDiv
		for j, c := range classes {
			if c.size >= size {
				sc[i] = j
				break
			}
		}
	}
	fmt.Fprint(w, "var size_to_class8 = [smallSizeMax/smallSizeDiv+1]uint8 {")
	for _, v := range sc {
		fmt.Fprintf(w, "%d,", v)
	}
	fmt.Fprintln(w, "}")

	// map from size to size class, for large sizes.
	sc = make([]int, (maxSmallSize-smallSizeMax)/largeSizeDiv+1)
	for i := range sc {
		size := smallSizeMax + i*largeSizeDiv
		for j, c := range classes {
			if c.size >= size {
				sc[i] = j
				break
			}
		}
	}
	fmt.Fprint(w, "var size_to_class128 = [(_MaxSmallSize-smallSizeMax)/largeSizeDiv+1]uint8 {")
	for _, v := range sc {
		fmt.Fprintf(w, "%d,", v)
	}
	fmt.Fprintln(w, "}")
}
