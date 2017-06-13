package main

import (
	"fmt"
	"github.com/gomqtt/packet"
	"net"
	"time"
)

func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("socket create failed")
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
		}
		go session(conn)
	}
}

type Session struct {
	ID            uint           `gorm:"primary_key"`
	ClientID      string         `gorm:"size:65535;not null;unique_index`
	Username      string         `gorm:"size:65535;not null"`
	Password      string         `gorm:"size:65535;not null"`
	CleanSession  bool           `gorm:"not null"`
	Will          Message        `gorm:"ForeignKey:SessionId"`
	QosMessages   []Message      `gorm:"ForeignKey:SessionId"`
	Subscriptions []Subscription `gorm:"ForeignKey:SessionId"`

	C      net.Conn      `gorm:"-"`
	Stream packet.Stream `gorm:"-"`
}
type Subscription struct {
	ID        uint `gorm:"primary_key"`
	SessionID uint
	Topic     string `gorm:"size:65535;not null`
	QOS       uint8  `gorm:"not null"`
}
type Message struct {
	ID        uint `gorm:"primary_key"`
	SessionID uint
	Topic     string `gorm:"size:65535"`
	Payload   string `gorm:"size:65535"`
	QOS       byte   `gorm:"not null"`
	Retain    bool   `gorm:"not null"`
}

func (s *Session) ProcessSession() error {
	first := true
	s.c.SetReadDeadline(time.Now().Add(1 * time.Second))

	for {
		pkt, err := s.Stream.Decoder.Read()
		if err != nil {
			fmt.Println(err)
			break
		}
		if first {
			connect, ok := pkt.(*packet.ConnectPacket)
			if !ok {
				fmt.Println("connect packet error")
				break
			}
			err = s.processConnect(connect)
			first = false
		}
		switch _pkt := pkt.(type) {
		case *packet.SubscribePacket:
			err = s.processSubscribe(_pkt)
		case *packet.UnsubscribePacket:
			err = s.processUnsubscribe(_pkt)
		case *packet.PublishPacket:
			err = s.processPublic(_pkt)
		case *packet.PubackPacket:
			err = s.processPuback(_pkt)
		case *packet.PubrecPacket:
			err = s.processPubrec(_pkt)
		case *packet.PubrelPacket:
			err = s.processPublic(_pkt)
		case *packet.PubcompPacket:
			err = s.processPubcomp(_pkt)
		case *packet.PingreqPacket:
			err = s.processPingreq(_pkt)
		case *packet.DisconnectPacket:
			err = s.processDisconnect(_pkt)
		}

	}

}
func (s *Session) QuerySession() bool {
	db, err := OpenDb()
	defer db.Close()
	return !db.Where("client_id=?", s.ClientID).First(&Session{}).RecordNotFound()
}
func (s *Session) SaveSession() error {
	db, err := OpenDb()
	defer db.Close()
	db.Create(s)
}
func (s *Session) DeleteSession() error {
	db, err := OpenDb()
	defer db.Close()
	var _s Session
	db.Where("client_id=?", s.ClientID).First(&_s)
	//delete the will message
	var mes Message
	db.Model(&_s).Related(&mes, "Will")
	if mes != nil {
		db.Delete(&mes)
	}
	//delete the qos message
	var qosmessages []Message
	db.Model(&client).Related(&qosmessage, "QosMessages")
	for _, qosmessage := range qosmessages {
		db.Delete(&qosmessage)
	}
	//delete the sub topic
	var subscriptions []Subscription
	db.Model(&client).Related(&subscriptions, "Subscriptions")
	for _, sub := range subscriptions {
		db.Delete(&sub)
	}
	//delete the session
	db.Delete(&_s)
}
func (s *Session) SetupSession(connect *packet.ConnectPacket) {
	s.CleanSession = connect.CleanSession
	s.ClientID = connect.ClientID
	s.Username = connect.Username
	s.Password = connect.Password
	if connect.Will != nil {
		s.Will.Payload = connect.Will.Payload
		s.Will.Retain = connect.Will.Retain
		s.Will.Topic = connect.Will.Topic
		s.Will.QOS = connect.Will.QOS
	}
}
func (s *Session) processConnect(connect *packet.ConnectPacket) error {
	connack := packet.NewConnackPacket()
	connack.ReturnCode = packet.ConnectionAccepted
	connack.SessionPresent = false
	s.SetupSession(connect)
	//auth todo
	if s.QuerySession() {
		//indb
		if connect.CleanSession {
			s.DeleteSession()
			s.SaveSession()
		} else {
			s.UpdateSession()
		}
	} else {
		s.SaveSession()
	}

}
func (s *Session) processSubscribe(sub *packet.SubscribePacket) error {

}
func (s *Session) processUnsubscribe(unsub *packet.UnsubscribePacket) error {

}
func (s *Session) processPublic(public *packet.PublishPacket) error {

}
func (s *Session) processPuback(puback *packet.PubackPacket) error {

}
func (s *Session) processPubrec(pubrec *packet.PubrecPacket) error {

}
func (s *Session) processPubrel(pubrel *packet.PubrelPacket) error {

}
func (s *Session) processPubcomp(pubcomp *packet.PubcompPacket) error {

}
func (s *Session) processPingreq(preq *packet.PingreqPacket) error {

}
func (s *Session) processDisconnect(disconnect *packet.DisconnectPacket) error {

}
func NewSession(c net.Conn) *Session {
	s := &Session{
		C:      c,
		Stream: packet.NewStream(socket, socket),
	}
	go s.ProcessSession()
	return nil
}
