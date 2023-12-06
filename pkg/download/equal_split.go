package download

func EqualSplit(number int64, parts int64) []int64 {
	output := make([]int64, parts)
	quotient := number / parts
	remainder := number % parts
	i := int64(0)
	for ; i < remainder; i++ {
		output[i] = quotient + 1
	}
	for ; i < parts; i++ {
		output[i] = quotient
	}
	return output
}
