package tcp

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"redis-go/interface/tcp"
	"redis-go/lib/sync/atomic"
	"sync"
	"syscall"
	"time"

	logger "github.com/sirupsen/logrus"
)

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)

	return nil
}

func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	var closing atomic.Boolean
	go func() {
		<-closeChan
		logger.Info("shutting down...")
		closing.Set(true)
		_ = listener.Close()
		_ = handler.Clos()
	}()

	defer func() {
		_ = listener.Close()
		_ = handler.Clos()
	}()

	ctx := context.Background()
	var waitGroup sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			// 如果处于 close 状态
			if closing.Get() {
				logger.Info("waiting disconnect...")
				// 等所有客户连接处理完成
				waitGroup.Wait()
				return
			}
			logger.Error(fmt.Sprintf("accept err: %v", err))
			continue
		}
		logger.Info("accept link")
		waitGroup.Add(1)
		go func() {
			defer func() {
				waitGroup.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
}
