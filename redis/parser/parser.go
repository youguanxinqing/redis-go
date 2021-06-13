package parser

import (
	"bufio"
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

type readState struct {
	downloading       bool
	expectedArgsCount int
	receivedCount     int
	msgType           byte
	args              [][]byte
	fixedLen          int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && s.expectedArgsCount == s.receivedCount
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
		if !state.downloading {
			switch msg[0] {
			case '*': // multi bulk reply
			case '$': // bulk reply
			default: // single line reply
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				continue
			}
		} else {

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

	if state.fixedLen == 0 {
		msg, err = bufReader.ReadBytes('\n')
		if err != nil { // read normal line
			return nil, true, err
		}
		// 确保 '\r\n' 结尾
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else { // read bulk line (binary safe)
		msg = make([]byte, state.fixedLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 ||
			msg[len(msg)-2] == '\r' ||
			msg[len(msg)-1] == '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.fixedLen = 0
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
