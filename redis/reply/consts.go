package reply

// emptyMultiBulkBytes present empty list
var emptyMultiBulkBytes = []byte("*0\r\n")

type EmptyMultiBulkReply struct{}

func (*EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

// nullBulkBytes present empty string
var nullBulkBytes = []byte("$-1\r\n")

type NullBulkReply struct{}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

func (*NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}
