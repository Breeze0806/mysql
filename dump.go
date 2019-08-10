package mysql

import (
	"context"
	"encoding/binary"
)

//DumpConn dump协议获取binlog
type DumpConn struct {
	*mysqlConn
}

var (
	PacketEOF = iEOF
	PacketOK  = iOK
	PacketERR = iERR
)

//NewDumpConn 获取协议获取binlog
func NewDumpConn(dsn string, ctx context.Context) (*DumpConn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	mc, err := c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &DumpConn{
		mysqlConn: mc.(*mysqlConn),
	}, nil
}

//Close 关闭连接
func (d *DumpConn) Close() error {
	return d.mysqlConn.Close()
}

//Exec 执行语句
func (d *DumpConn) Exec(query string) error {
	return d.exec(query)
}

//NoticeDump 通知协议使用dump协议
func (d *DumpConn) NoticeDump(serverID, offset uint32, filename string, flags uint16) error {
	return d.writeDumpBinlogPosPacket(serverID, offset, filename, flags)
}

//ReadPacket 读取binlog消息
func (d *DumpConn) ReadPacket() ([]byte, error) {
	return d.readPacket()
}

//ReadPacket 处理回复包错误
func (d *DumpConn) HandleErrorPacket(data []byte) error {
	return d.handleErrorPacket(data)
}

func (d *DumpConn) writeDumpBinlogPosPacket(serverID, offset uint32, filename string, flags uint16) error {
	d.sequence = 0
	length := 4 + //header
		1 + // ComBinlogDump
		4 + // binlog-pos
		2 + // flags
		4 + // server-id
		len(filename) // binlog-filename
	data, err := d.buf.takeSmallBuffer(length)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return errBadConnNoWrite
	}

	pos := writeByte(data, 4, comBinlogDump)
	pos = writeUint32(data, pos, uint32(offset))
	pos = writeUint16(data, pos, flags)
	pos = writeUint32(data, pos, serverID)
	writeEOFString(data, pos, filename)

	return d.writePacket(data)
}

func writeEOFString(data []byte, pos int, value string) int {
	pos += copy(data[pos:], value)
	return pos
}

func writeByte(data []byte, pos int, value byte) int {
	data[pos] = value
	return pos + 1
}

func writeUint16(data []byte, pos int, value uint16) int {
	binary.LittleEndian.PutUint16(data[pos:], value)
	return pos + 2
}

func writeUint32(data []byte, pos int, value uint32) int {
	binary.LittleEndian.PutUint32(data[pos:], value)
	return pos + 4
}
