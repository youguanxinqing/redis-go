package parser

import (
	"bufio"
	"bytes"
	"errors"
	logger "github.com/sirupsen/logrus"
	"io"
	"redis-go/interface/redis"
	"redis-go/redis/reply"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream reads from io.Reader and send Payload though channel
func ParseStream(reader io.Reader) chan<- *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)

	var results []redis.Reply
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				break
			}
			return nil, payload.Err
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse0(reader, ch)

	payload, ok := <-ch
	if !ok {
		return nil, errors.New("no reply")
	}
	return payload.Data, payload.Err
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && s.expectedArgsCount == len(s.args)
}

// parse0 解析客户端协议
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()

	bufReader := bufio.NewReader(reader)
	var (
		state readState
		err   error
		msg   []byte
	)
	for {
		var isIoErr bool

		msg, isIoErr, err = readLine(bufReader, &state)
		if err != nil {
			if isIoErr { // io err, stop read
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// protocol err, reset read state
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		// parse line
		/*
			RESP 通过第一个字符来表示格式：
			- 简单字符串：以"+" 开始， 如："+OK\r\n"
			- 错误：以"-" 开始，如："-ERR Invalid Syntax\r\n"
			- 整数：以":"开始，如：":1\r\n"
			- 字符串：以 $ 开始
			- 数组：以 * 开始
		*/
		if !state.readingMultiLine {
			switch msg[0] {
			case '*': // multi bulk reply
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					state = readState{} // reset state
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			case '$': // bulk reply
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					state = readState{}
					continue
				}
				if state.bulkLen == -1 {
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{}
					continue
				}
			default: // single line reply
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else { // receive following bulk reply
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				state = readState{}
				continue
			}
			if state.finished() {
				var result redis.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

// readLine
// returns:
// - []byte: 行内容
// - bool:   是否 IO 异常
// - error:
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var (
		msg []byte
		err error
	)
	if state.bulkLen == 0 {
		msg, err = bufReader.ReadBytes('\n')
		if err != nil { // read normal line
			return nil, true, err
		}
		// 确保 '\r\n' 结尾
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // read bulk line (binary safe)
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 ||
			msg[len(msg)-2] == '\r' ||
			msg[len(msg)-1] == '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}

	return msg, false, nil
}

///////////////////////// tools /////////////////////////

//- 简单字符串：以"+" 开始， 如："+OK\r\n"
//- 错误：以"-" 开始，如："-ERR Invalid Syntax\r\n"
//- 整数：以":"开始，如：":1\r\n"
func parseSingleLineReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")

	var result redis.Reply
	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	default:
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = reply.MakeMultiBulkReply(args)
	}
	return result, nil
}

//- 数组以"*"开头, "*3"表示数组中有 3 个元素
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var (
		err          error
		expectedLine uint64
	)
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil || expectedLine < 0 {
		return errors.New("protocol error: " + string(msg))
	}
	state.expectedArgsCount = int(expectedLine)
	if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.args = make([][]byte, 0, expectedLine)
	}
	return nil
}

//- 字符串(Bulk String)以"$"开头,
//- "$1"表示 nil, 例如 get 命令查询一个不存在的 key 时,返回 $1
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-1]
	var err error
	if line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { // null bulk only in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
