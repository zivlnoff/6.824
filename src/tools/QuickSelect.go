package tools

func swap(arr []int, i int, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

func Quick_select(arr []int, n int) int {
	var low, high int
	var median int
	var middle, ll, hh int

	low = 0
	high = n - 1
	median = (low + high) / 2
	for {
		if high <= low /* One element only */ {
			return arr[median]
		}

		if high == low+1 { /* Two elements only */
			if arr[low] > arr[high] {
				swap(arr[:], low, high)
			}
			return arr[median]
		}

		/* Find median of low, middle and high items; swap into position low */
		middle = (low + high) / 2
		if arr[middle] > arr[high] {
			swap(arr[:], middle, high)
		}
		if arr[low] > arr[high] {
			swap(arr[:], low, high)
		}

		if arr[middle] > arr[low] {
			swap(arr[:], middle, low)
		}

		/* swap low item (now in position middle) into position (low+1) */
		swap(arr[:], middle, low+1)

		/* Nibble from each end towards middle, swapping items when stuck */
		ll = low + 1
		hh = high
		for {
			for {
				ll++
				if arr[low] <= arr[ll] {
					break
				}

			}

			for {
				hh--
				if arr[hh] <= arr[low] {
					break
				}
			}

			if hh < ll {
				break
			}

			swap(arr[:], ll, hh)
		}

		/* swap middle item (in position low) back into correct position */
		swap(arr[:], low, hh)

		/* Re-set active partition */
		if hh <= median {
			low = ll
		}

		if hh >= median {
			high = hh - 1
		}
	}
}
