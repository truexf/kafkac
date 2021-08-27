package kafkac

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/truexf/gobuffer"
	"github.com/truexf/goutil"
)

type FileRecover struct {
	topic         string
	path          string
	closeNotify   chan struct{}
	bufWriter     *gobuffer.GoBuffer
	restoreNotify chan RestoreFunc
}

func NewFileRecover(topic, savePath string, bufCap int, writeFileIntervalSecond int) (*FileRecover, error) {
	if !goutil.FilePathExists(savePath) {
		return nil, fmt.Errorf("savePath: %s not exists", savePath)
	}
	topicPath := filepath.Join(savePath, topic)
	if !goutil.FilePathExists(topicPath) {
		if err := os.Mkdir(topicPath, 0777); err != nil {
			return nil, fmt.Errorf("makedir %s fail, %s", topicPath, err.Error())
		}
	}
	if bufCap <= 0 {
		bufCap = 1024 * 1024 * 64
	}

	ret := &FileRecover{
		topic:         hex.EncodeToString([]byte(topic)), //防止与正则表达式冲突
		path:          topicPath,
		closeNotify:   make(chan struct{}),
		restoreNotify: make(chan RestoreFunc),
	}
	if writeFileIntervalSecond < 1 {
		writeFileIntervalSecond = 1
	}
	//每分钟一个文件
	filePrefix := fmt.Sprintf("_kafka_%s_", ret.topic)
	goBuf, err := gobuffer.NewTimedFileWriterWithGoBuffer2(bufCap, writeFileIntervalSecond, topicPath, filePrefix, "2006-01-02-15-04.data")
	if err != nil {
		return nil, fmt.Errorf("create file writer fail, %s", err.Error())
	}
	ret.bufWriter = goBuf

	go func() {
		closed := false
		for {
			select {
			case <-ret.closeNotify:
				closed = true
			case fn := <-ret.restoreNotify:
				ret.doRestore(fn)
			}
			if closed {
				break
			}
		}
		ret.bufWriter.Flush()
	}()

	return ret, nil

}

func (m *FileRecover) Save(message Message) error {
	keyLen := base64.StdEncoding.EncodedLen(len(message.Key))
	valueLen := base64.StdEncoding.EncodedLen(len(message.Value))
	dataCap := keyLen + 1 + valueLen + 1
	msg := make([]byte, dataCap)
	if keyLen > 0 {
		base64.StdEncoding.Encode(msg, message.Key)
	}
	msg[keyLen] = ':'
	if valueLen > 0 {
		base64.StdEncoding.Encode(msg[keyLen+1:], message.Value)
	}
	msg[dataCap-1] = '\n'
	_, err := m.bufWriter.Write(msg)
	return err
}

func (m *FileRecover) Restore(fn RestoreFunc) {
	m.restoreNotify <- fn
}
func (m *FileRecover) doRestore(fun RestoreFunc) {
	regStr := `^\_kafka\_` + m.topic + `\_[0-9]{4}\-[0-9]{2}\-[0-9]{2}\-[0-9]{2}\-[0-9]{2}\.data$`
	regExp := regexp.MustCompile(regStr)
	files, err := ioutil.ReadDir(m.path)
	canceled := false
	if err != nil {
		fun(nil, err, &canceled)
		return
	}
	for _, v := range files {
		if v.IsDir() {
			continue
		}
		if !regExp.MatchString(v.Name()) {
			continue
		}
		if v.Size() <= 0 {
			os.Remove(filepath.Join(m.path, v.Name()))
			continue
		}
		fn := filepath.Join(m.path, v.Name())
		bst, err := ioutil.ReadFile(fn)
		if err != nil {
			fun(nil, fmt.Errorf("read data from file %s fail %s", fn, err.Error()), &canceled)
			return
		}
		if len(bst) == 0 {
			os.Remove(fn)
			continue
		}
		dataList := bytes.Split(bst, []byte{'\n'})
		for i, v := range dataList {
			if len(v) == 0 {
				continue
			}
			splitPos := bytes.Index(v, []byte{':'})
			if splitPos < 0 {
				continue
			}
			key := v[:splitPos]
			value := v[splitPos+1:]
			nKey := base64.StdEncoding.DecodedLen(len(key))
			nValue := base64.StdEncoding.DecodedLen(len(value))
			msg := &Message{}
			if nKey > 0 {
				msg.Key = make([]byte, nKey)
			}
			if nValue > 0 {
				msg.Value = make([]byte, nValue)
			}
			keyLen, err := base64.StdEncoding.Decode(msg.Key, key)
			if err == nil {
				msg.Key = msg.Key[:keyLen]
			} else {
				continue
			}
			valueLen, err := base64.StdEncoding.Decode(msg.Value, value)
			if err == nil {
				msg.Value = msg.Value[:valueLen]
			} else {
				continue
			}
			fun(msg, err, &canceled)
			if canceled {
				if i < len(dataList)-1 {
					remainData := bytes.Join(dataList[i+1:], []byte{'\n'})
					ioutil.WriteFile(fn, remainData, 0666)
				} else {
					os.Remove(fn)
				}
				return
			}
		}
		os.Remove(fn)
	}
}

func (m *FileRecover) CloseNotify() chan struct{} {
	return m.closeNotify
}
