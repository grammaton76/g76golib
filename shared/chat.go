package shared

import (
	"fmt"
	"github.com/grammaton76/g76golib/sjson"
	"github.com/grammaton76/g76golib/slogger"
	"os"
	"reflect"
	"strings"
	"time"
)

var ChatHandleConfigs ChatHandleConfigMap

type ChatHandleConfigMap map[string]*ChatTypeConfig

type ChatTypeConfig struct {
	BindFunc func(*ChatHandle, *Configuration, string) error
}

type ChatHandle struct {
	Validate            bool
	Sender              string
	key                 string
	section             string
	hasconnected        bool
	ErrorChannel        *ChatTarget
	OutputChannel       *ChatTarget
	PrintChatOnly       bool
	DirectClient        *DirectClient
	failed              error
	warnings            []error
	DirectData          interface{}
	ChatType            ChatConnectType
	Owner               string
	UserIndex           UserIndexType
	RefuseMessagesAfter time.Duration
	ChannelLookup       map[string]*ChatTarget
	UserLookup          map[string]*UserInfo
	NativeClient        interface{}
	deferConfig         *sjson.JSON
	FpSendSimple        func(*ChatHandle, *ChatTarget, string) (*ChatUpdateHandle, error)
	FpSendMessage       func(*ChatHandle, *ChatMessage) (*ChatUpdateHandle, error)
	FpFormulateStimulus func(*ChatHandle, *MessagerEvent) *ResponseTo
	FpChatTargetChannel func(*ChatHandle, string) *ChatTarget
	FpChatTargetUser    func(*ChatHandle, string) *ChatTarget
	FpJoinChannel       func(*ChatHandle, *ChatTarget) error
	FpUserById          func(*ChatHandle, string) *UserInfo
	FpNewListener       func(*ChatHandle) (*ListenHandle, error)
	FpSendImageTarget   func(*ChatTarget, *Image) error
	FpIdentifier        func(*ChatHandle) string
	FpGetMsgRefByLabel  func(*ChatHandle, *ChatTarget, string) *ChatUpdateHandle
	FpGetMsgByLabel     func(*ChatHandle, *ChatTarget, string) *ChatMessage
}

type ResponseTo struct {
	Target        *ChatTarget
	Sender        *UserInfo
	FromAdmin     bool
	InThread      string
	IsDM          bool
	Arguments     []string
	LcArguments   []string
	OrigMessage   string
	Message       string
	MsgId         string
	NativeMessage interface{}
}

type ChatMessage struct {
	handle       *ChatHandle
	MsgType      ChatType
	Sender       string
	Target       *ChatTarget
	Label        string
	Message      string
	LabelIfReply string
	UpdateHandle *ChatUpdateHandle
	History      struct {
		Posted *time.Time
	}
	Segments SegmentedMsg
	Table    [][]string
	Options  *sjson.JSON
	queued   []string
}

type MessagerEvent struct {
	MsgId        string
	ParentMsgId  string
	ChannelId    string
	SenderUserId string // Account level, not connection level
	Text         string
	MsgType      MsgType
	Connection   interface{}
	NativeMsg    interface{}
	Timestamp    *time.Time
	FpToStimulus func(*MessagerEvent) *ResponseTo
}

const (
	SEGTYPE_UNDEF  MsgSegmentType = 0
	SEGTYPE_TEXT   MsgSegmentType = 1
	SEGTYPE_IMGURL MsgSegmentType = 2
	SEGTYPE_LABEL  MsgSegmentType = 3
)

type MsgSegmentType int

type MsgSegment struct {
	Text     string
	Itemtype MsgSegmentType
}

type SegmentedMsg []MsgSegment

type MsgType int

const (
	MsgTypeDM      MsgType = 1
	MsgTypeChannel MsgType = 2
)

type ChatTarget struct {
	Handle    *ChatHandle
	Native    interface{}
	failed    error
	Id        string
	Name      string
	IsDM      bool
	IsMpDm    bool
	IsAdmin   bool
	IsPrivate bool
	IsPublic  bool
	PrintOnly bool
}

type ConnectingEvent struct{}
type InvalidAuthEvent struct{}

type ChatOptions struct {
	Mediaunfurl bool        `json:"mediaunfurl"`
	Linkunfurl  bool        `json:"linkunfurl"`
	Blocks      interface{} `json:"blocks"`
	Parse       bool
	IconUrl     string `json:"iconurl"`
	IconEmoji   string
	Attachtext  string
	SetTopic    string
	PinPost     bool       `json:"PinPost"`
	UnpinPost   bool       `json:"UnpinPost"`
	RespondTo   int64      `json:"respondto"`
	ThreadLabel string     `json:"threadlabel"`
	UpdateLabel string     `json:"updatelabel"`
	UpdateMsgId string     `json:"updatemsgid"`
	Columns     [][]string `json:"columns"`
	NoText      bool
}

type DirectClient struct {
	ChatType            ChatConnectType
	Owner               string
	UserIndex           UserIndexType
	RefuseMessagesAfter time.Duration
	ChannelLookup       map[string]*ChatTarget
	NativeClient        interface{}
	FpSendSimple        func(*DirectClient, *ChatTarget, string) (string, string, error)
	FpSendMessage       func(*DirectClient, *ChatMessage) (*ChatUpdateHandle, error)
	FpFormulateStimulus func(*DirectClient, *MessagerEvent) *ResponseTo
	FpChatTargetChannel func(*DirectClient, string) *ChatTarget
	FpChatTargetUser    func(*DirectClient, string) *ChatTarget
	FpJoinChannel       func(*DirectClient, *ChatTarget) error
	FpUserById          func(*DirectClient, string) *UserInfo
	FpManageConnection  func(*DirectClient)
	FpIdentifier        func(*DirectClient) string
}

type ListenHandle struct {
	Dapi               *ChatHandle
	Native             interface{}
	Incoming           chan interface{}
	failures           []error
	FpManageConnection func(handle *ListenHandle)
}

type UserInfo struct {
	CorpId   string
	NativeId string
	Ignore   bool
	IsAdmin  bool
	IsBot    bool
	Email    string
	Name     string
	ChatId   string
	PermVal  sjson.JSON
	TempVal  sjson.JSON
	Native   interface{}
}

type UserIndexType struct {
	ById       map[string]*UserInfo
	RoomUserId map[string]*UserInfo
}

type ChatType int
type ChatConnectType int

const (
	ChatTypeUndef       ChatConnectType = 0
	ChatTypeDb          ChatConnectType = 1
	ChatTypeSlackDirect ChatConnectType = 2
	ChatTypeXmppDirect  ChatConnectType = 3
)

func (c ChatConnectType) String() string {
	switch c {
	case ChatTypeDb:
		return "dbtable"
	case ChatTypeSlackDirect:
		return "slack"
	case ChatTypeXmppDirect:
		return "xmpp"
	default:
		return "unknown"
	}
}

func ChatTypeFromString(s string) ChatConnectType {
	switch s {
	case "dbtable":
		return ChatTypeDb
	case "slack":
		return ChatTypeSlackDirect
	case "xmpp":
		return ChatTypeXmppDirect
	default:
		return ChatTypeUndef
	}

}

const (
	MsgNewPost      ChatType = 0
	MsgUpdate       ChatType = 1
	MsgPostOrUpdate ChatType = 2
	MsgPostOrReply  ChatType = 3
	MsgNoPost       ChatType = 4
	MaxChatSize     int      = 40960
)

type ChatUpdateHandle struct {
	NativeData interface{}
	ChannelId  string
	Timestamp  string
	RowId      int64
	MsgId      int64
	UpdateId   int64
}

func (c ChatHandleConfigMap) Lists() string {
	var Caw []string
	for v := range c {
		Caw = append(Caw, v)
	}
	buf := strings.Join(Caw, ", ")
	return buf
}

func (cth *ChatHandle) Warnings() []error {
	return cth.warnings
}

func (Seg MsgSegment) Type() string {
	switch Seg.Itemtype {
	case SEGTYPE_IMGURL:
		return "imgurl"
	case SEGTYPE_TEXT:
		return "text"
	case SEGTYPE_LABEL:
		return "label"
	case SEGTYPE_UNDEF:
		return "undef"
	default:
		return "ERROR-undefined"
	}
}

func (dc *DirectClient) SendSimple(target *ChatTarget, message string) (string, string, error) {
	return dc.FpSendSimple(dc, target, message)
}

func (Seg MsgSegment) String() string {
	return fmt.Sprintf("%s: %s", Seg.Type(), Seg.Text)
}

func (tgt *ChatTarget) Identifier() string {
	if tgt == nil {
		return "ChatTarget:NIL"
	}
	return tgt.Id
}

func (dc *DirectClient) Join(target *ChatTarget) error {
	return dc.FpJoinChannel(dc, target)
}

func (dc *DirectClient) FormulateStimulus(event *MessagerEvent) *ResponseTo {
	return dc.FpFormulateStimulus(dc, event)
}

func (dc *DirectClient) Identifier() string {
	if dc.FpIdentifier == nil {
		return fmt.Sprintf("nil FpIdentifier() on dc.\n")
	}
	return dc.FpIdentifier(dc)
}

func (dc *DirectClient) ChatTargetUser(id string) *ChatTarget {
	if dc.FpChatTargetUser == nil {
		log.Errorf("Called ChatTargetUser for '%s' without a valid method!\n", dc.Identifier())
		return nil
	}
	return dc.FpChatTargetUser(dc, id)
}

func (dc *DirectClient) ChatTargetChannel(id string) *ChatTarget {
	if dc.FpChatTargetChannel != nil {
		return dc.FpChatTargetChannel(dc, id)
	}
	log.Fatalf("Client %s has no defined FpChatTargetChannel() function.\n", dc.Identifier())
	return nil
}

func (dc *DirectClient) UserById(id string) *UserInfo {
	if dc.FpUserById != nil {
		return dc.FpUserById(dc, id)
	}
	log.Fatalf("Client %s has no defined FpUserById() function.\n", dc.Identifier())
	return nil
}

/*
func (dc *DirectClient) NewListener() *ListenHandle {
	if dc.FpNewListener!=nil {
		return dc.FpNewListener(dc)
	}
	log.Fatalf("Client %s has no defined FpNewListener() function.\n", dc.Identifier())
	return nil
}*/

func (cth *ChatHandle) SetValidate() {
	cth.Validate = true
	validator := sjson.NewJson()
	validator.IngestFromObject(*cth)
	for k := range validator {
		Type := reflect.ValueOf(k).Type().String()
		switch Type {
		default:
			log.Printf("Type: %s\n", Type)
		}
	}
	os.Exit(3)
}

func (tgt *ChatTarget) SendfIfDef(format string, options ...interface{}) {
	if tgt == nil {
		log.Debugf("SendfIfdef() on nil ChatTarget; returning.\n")
		return
	}
	tgt.Sendf(format, options...)
}

func (dc *DirectClient) SendMessage(Chat *ChatMessage) (*ChatUpdateHandle, error) {
	return dc.FpSendMessage(dc, Chat)
}

func (dc *DirectClient) SendSimpleChannel(channel string, message string) (string, string, error) {
	return dc.FpSendSimple(dc, dc.ChatTargetChannel(channel), message)
}

func (lh *ListenHandle) ManageConnection() {
	if lh.FpManageConnection == nil {
		return
	}
	lh.FpManageConnection(lh)
}

func (tgt *ChatTarget) Sendf(format string, options ...interface{}) {
	if tgt == nil {
		log.Fatalf("Error: nil ChatTarget on Sendf()\n")
	}
	Msg := fmt.Sprintf(format, options...)
	tgt.Handle.SendSimple(tgt, Msg)
	//		Sendf(format, options)
}

func (tgt *ChatTarget) SendImageTarget(Image *Image) error {
	if tgt == nil {
		return fmt.Errorf("nil ChatTarget on SendImageTarget()")
	}
	return tgt.Handle.FpSendImageTarget(tgt, Image)
}

func (tgt *ChatTarget) Send(Msg *ChatMessage) (*ChatUpdateHandle, error) {
	if tgt == nil {
		return nil, fmt.Errorf("nil ChatTarget on Send()")
	}
	if tgt.Handle == nil {
		return nil, fmt.Errorf("nil ChatTarget parent handle on Send()")
	}
	Msg.Target = tgt
	return tgt.Handle.Send(Msg)
}

func (cth *ChatHandle) ChatTargetUser(user string) *ChatTarget {
	return cth.FpChatTargetUser(cth, user)
}

func (cth *ChatHandle) FormulateStimulus(event *MessagerEvent) *ResponseTo {
	return cth.FpFormulateStimulus(cth, event)
}

func (tgt *ChatTarget) OrDie(msgs ...string) *ChatTarget {
	if tgt == nil {
		log.Fatalf("nil ChatTarget")
	}
	if tgt.failed == nil {
		return tgt
	}
	if tgt.Handle == nil {
		log.Warnf("Should be impossible: ChatTarget '%s' has nil handle.\n", tgt.Identifier())
	} else {
		if tgt.Handle.failed != nil {
			log.Printf("Failures present in parent chathandle: %s", tgt.Handle.failed)
		} else {
			log.Printf("No failures present in parent chathandle\n")
		}
	}
	log.FatalIff(tgt.failed, "Failed to create chat target: %s\n%s\n",
		tgt.Identifier(), strings.Join(msgs, "\n"))
	return nil
}

func (cth *ChatHandle) ChatTarget(target string) *ChatTarget {
	var Ret *ChatTarget
	if cth.PrintChatOnly {
		Ret = &ChatTarget{
			Handle:    cth,
			Id:        target,
			Name:      target,
			PrintOnly: cth.PrintChatOnly,
		}
		return Ret
	}
	if cth.FpChatTargetChannel == nil && cth.FpChatTargetUser == nil {
		Ret = &ChatTarget{PrintOnly: true}
		Ret.failed = fmt.Errorf("%s: no functions to spawn target object for %s\n", cth.Identifier(), target)
		log.Errorf("%s: no functions to spawn target object for %s\n", cth.Identifier(), target)
		return Ret
	}
	if cth.FpChatTargetChannel != nil {
		Ret = cth.FpChatTargetChannel(cth, target)
	}
	if Ret == nil {
		if cth.FpChatTargetUser != nil {
			Ret = cth.FpChatTargetUser(cth, target)
		}
	}
	if Ret == nil {
		log.Printf("Unable to translate '%s' into a chat target for '%s'\n", target, cth.Identifier())
	} else {
		Ret.Handle = cth
	}
	return Ret
}

func (cth *ChatHandle) ChatTargetChannel(id string) *ChatTarget {
	if cth.FpChatTargetChannel == nil {
		log.Fatalf("FpChatTargetChannel undefined for '%s'\n", cth.Identifier())
	}
	return cth.FpChatTargetChannel(cth, id)
}

func (cth *ChatHandle) UserById(id string) *UserInfo {
	if cth.FpUserById != nil {
		return cth.FpUserById(cth, id)
	}
	log.Fatalf("cth %s had nil FpUserById()", cth.Identifier())
	return nil
}

func (cth *ChatHandle) NewListener() (*ListenHandle, error) {
	if cth.FpNewListener != nil {
		return cth.FpNewListener(cth)
	}
	return nil, fmt.Errorf("FpNewListener() not defined")
}

func (cth *ChatHandle) Join(target *ChatTarget) error {
	return cth.FpJoinChannel(cth, target)
}

func (cth *ChatHandle) SendMessage(Chat *ChatMessage) (*ChatUpdateHandle, error) {
	return cth.FpSendMessage(cth, Chat)
}

func (cth *ChatHandle) SendSimpleChannel(channel string, message string) (*ChatUpdateHandle, error) {
	return cth.FpSendSimple(cth, cth.ChatTargetChannel(channel), message)
}

func (cth *ChatHandle) SendSimple(target *ChatTarget, message string) (*ChatUpdateHandle, error) {
	return cth.FpSendSimple(cth, target, message)
}

func (cth *ChatHandle) OrDie(msgs ...string) *ChatHandle {
	log.FatalIff(cth.failed, "Failed to prepare statement: %s\nSQL: %s\n%s\n",
		cth.Identifier(), strings.Join(msgs, "\n"))
	return cth
}

func (cth *ChatHandle) Identifier() string {
	if cth == nil {
		return "(null chat handle)"
	}
	if cth.FpIdentifier != nil {
		return cth.FpIdentifier(cth)
	}
	var Caw string
	Caw = "(unknown chat handle)"
	if cth.key != "" {
		Caw = fmt.Sprintf("chattype-%d handle defined in section '%s', as defined by key '%s'", cth.ChatType, cth.section, cth.key)
	} else if cth.section != "" {
		Caw = fmt.Sprintf("chattype-%d handle defined by section '%s', no key reference.", cth.ChatType, cth.section)
	}
	return Caw
}

func (ev *MessagerEvent) Channel() *ChatTarget {
	return nil
}

func (ev *MessagerEvent) ToStimulus() *ResponseTo {
	if ev.FpToStimulus == nil {
		log.Fatalf("ev.FpToStimulus is nil for this messager event.\n")
	}
	return ev.FpToStimulus(ev)
}

func (cth *ChatHandle) ShowConfig() string {
	return fmt.Sprintf("Chat handle config: %+v", cth)
}

func (cth *ChatHandle) SetErrorChannel(Channel string) *ChatHandle {
	cth.ErrorChannel = cth.ChatTargetChannel(Channel)
	return cth
}

func (cth *ChatHandle) SetDefaultChannel(Channel string) *ChatHandle {
	cth.OutputChannel = cth.ChatTargetChannel(Channel)
	return cth
}

func (cth *ChatHandle) SetDefaultSender(Handle string) *ChatHandle {
	cth.Sender = Handle
	return cth
}

func (cth *ChatHandle) SendErrorf(format string, options ...interface{}) error {
	return cth.SendError(fmt.Sprintf(format, options...))
}

func (cth *ChatHandle) SendError(Message string) error {
	var err error
	if cth.PrintChatOnly {
		log.Infof("PRINTED error-chat message by '%s' to channel '%s': '%s'\n",
			cth.Sender, cth.ErrorChannel, Message)
		return nil
	}
	if cth.DirectClient != nil && cth.DirectClient.FpSendMessage != nil {
		var Msg ChatMessage
		Msg.Sender = cth.Sender
		Msg.Target = cth.ErrorChannel
		if Msg.Target == nil {
			return fmt.Errorf("nil errorchannel on %s", cth.Identifier())
		}
		Msg.Message = Message
		log.Printf("Message sent: %s\n", Msg.Message)
		_, err = cth.SendMessage(&Msg)
		log.ErrorIff(err, "directchat to channel '%s' on '%s' failed",
			cth.ErrorChannel.Name, cth.Identifier())
	}
	if err != nil {
		switch err.Error() {
		case "channel_not_found":
			err = fmt.Errorf("channel_not_found for error channel '%s' on '%s'", cth.ErrorChannel, cth.Identifier())
		}
	}
	return err
}

func NewChatMessage() *ChatMessage {
	Bob := ChatMessage{}
	return &Bob
}

func (Msg *ChatMessage) Append(str string) *ChatMessage {
	Msg.queued = append(Msg.queued, str)
	return Msg
}

func (cth *ChatHandle) Send(origmessage *ChatMessage) (*ChatUpdateHandle, error) {
	var Empty bool = true
	message := *origmessage
	if message.Options == nil {
		Bob := sjson.NewJson()
		message.Options = &Bob
	}
	if len(message.Table) > 0 {
		(*message.Options)["columns"] = message.Table
		Empty = false
	}
	if cth.PrintChatOnly {
		if message.Options != nil {
			log.Infof("SUPPRESSED chat message by '%s' to channel '%s': '%s' (options %s)\n",
				message.Sender, message.Target.Identifier(), message.Message, message.Options.String())
		} else {
			log.Infof("SUPPRESSED chat message by '%s' to channel '%s': '%s'\n",
				message.Sender, message.Target.Id, message.Message)
		}
		return nil, nil
	}
	if cth.FpSendMessage != nil {
		return cth.FpSendMessage(cth, &message)
	}

	if message.Target == nil {
		if cth.OutputChannel == nil {
			log.Errorf("No default channel (%s.channel) in ini, and nothing in message packet either.\n", cth.section)
			return nil, fmt.Errorf("no channel specified")
		}
		message.Target = cth.OutputChannel
	}
	if message.Sender == "" {
		message.Sender = cth.Sender
	}
	if message.Sender == "" {
		log.Errorf("No default sender (%s.chathandle) in ini, and no sender specified for chat!\n", cth.section)
		return nil, nil
	}
	var Update *ChatUpdateHandle
	var Label *string
	var LabelRef *ChatUpdateHandle
	var Options = sjson.NewJson()
	Options.IngestFromString(message.Options.String())
	if message.Label != "" {
		Label = &message.Label
		if cth.FpGetMsgByLabel != nil {
			LabelRef = cth.FpGetMsgRefByLabel(cth, message.Target, message.Label)
			log.Printf("Label requested was '%s' and id %d was obtained.\n", *Label, LabelRef.MsgId)
		}
	}
	if len(message.queued) > 0 {
		Empty = false
	}
	if len(*message.Options) > 0 {
		log.Printf("Options block contained %s\n", *message.Options)
	}
	log.Debugf("Received chat message to send.\nTarget: %s\n", origmessage.Target.Identifier())
	if message.Message == "" && Empty {
		log.Errorf("Blank message passed to chat; refusing to send.\n")
		return nil, fmt.Errorf("blank message; cannot send")
	}
	var LabelId int64
	if LabelRef != nil {
		LabelId = LabelRef.MsgId
	}
	switch message.MsgType {
	case MsgUpdate:
		if message.UpdateHandle.UpdateId == 0 && LabelId != 0 {
			message.UpdateHandle.UpdateId = LabelId
		}
	case MsgPostOrUpdate:
		if LabelId != 0 {
			message.UpdateHandle.UpdateId = LabelId
			message.MsgType = MsgUpdate
			log.Printf("Setting message type to UpdateMsgId because message %d was found for label %s.\n", LabelId, Label)
		}
	case MsgPostOrReply, MsgNewPost:
		if LabelId != 0 {
			message.MsgType = MsgNewPost
			Options["respondto"] = LabelId
			Options["respondlabel"] = Label
			Label = nil
			if message.LabelIfReply != "" {
				Label = &message.LabelIfReply
			}
		}
	default:
		log.Errorf("Message type not specified; action unclear.\n")
		return nil, fmt.Errorf("message type unspecified")
	}
	var Buffers []string
	if len(message.queued) > 0 {
		var Buf string
		for _, v := range message.queued {
			if (len(Buf) + len(v)) < MaxChatSize {
				Buf += v
				//log.Debugf("Compiling message '%s' from chat buffer\n", v)
			} else {
				if Buf != "" {
					Buffers = append(Buffers, Buf)
					Buf = ""
				}
			}
		}
		if len(Buffers) == 0 {
			if len(message.Message)+len(Buf) < MaxChatSize {
				message.Message += "\n" + Buf
				log.Debugf("Compiling message '%s' from chat buffer\n", message.Message)
			} else {
				Buffers = append(Buffers, Buf)
			}
		} else {
			if Buf != "" {
				Buffers = append(Buffers, Buf)
			}
		}
		Buf = ""
	}

	if LabelRef == nil {
		log.Debugf("Sending to channel %s: %s\n", message.Target.Id, message.Message)
		var err error
		var Sends []string
		if message.Message != "" {
			Sends = append(Sends, message.Message)
		}
		for _, v := range Buffers {
			Sends = append(Sends, v)
		}
		for _, v := range Sends {
			// message.Message = v
			Update, err = cth.FpSendSimple(cth, message.Target, v)
			if err != nil {
				log.Fatalf("ERROR on FpSendSimple: %s\n", err)
				return nil, err
			}
		}
	} else {
		var err error
		Update, err = cth.FpSendMessage(cth, &message)
		if err != nil {
			log.Fatalf("chat.chatMessageUpdate: %s\n", err)
		}
	}
	return Update, nil
}

func DieIfNil(Result bool, Msg string) {
	if Result {
		log.Fatalf("DieIfNil: %s\n", Msg)
	}
}

func (cth *ChatHandle) SendOut(Channel string, Message string) (*ChatUpdateHandle, error) {
	if cth == nil {
		log.Fatalf("ERROR: Called SendOut with a null chat handle!\n")
	}
	log.Debugf("SendOut: '%s' -> '%s'\n", Channel, Message)
	if Channel == "" {
		log.Infof("SUPPRESSED blank-channel chat message by '%s' to channel '%s': '%s'\n",
			cth.Sender, Channel, Message)
		return nil, fmt.Errorf("No channel specified for message sending text '%s'\n", Message)
	}
	if cth.PrintChatOnly {
		log.Infof("SUPPRESSED chat message by '%s' to channel '%s': '%s'\n",
			cth.Sender, Channel, Message)
		return nil, nil
	}
	if cth.Sender == "" {
		log.Infof("SUPPRESSED blank-sender chat message by '%s' to channel '%s': '%s'\n",
			cth.Sender, Channel, Message)
		return nil, fmt.Errorf("Blank sender '%s' on message '%s'\n", cth.Sender, Message)
	}
	if cth.Sender == "" {
		log.Fatalf("Can't do it; there is a blank Sender.\n")
	}
	return nil, nil
}

func (tgt *ChatTarget) LogFatalf(format string, options ...interface{}) {
	tgt.LogErrorf(format, options...)
	os.Exit(3)
}

func (tgt *ChatTarget) LogErrorf(format string, options ...interface{}) error {
	if tgt == nil {
		errstr := "nil chat handle passed to LogErrorf"
		log.Errorf(errstr)
	} else {
		tgt.Sendf(format, options...)
	}
	log.Printf("(Following message also sent to chat channel '%s')\n", tgt.Name)
	log.Errorf(format, options...)
	return fmt.Errorf(format, options...)
}

func (cth *ChatHandle) LogFatalf(format string, options ...interface{}) {
	cth.SendErrorf(format, options...)
	os.Exit(3)
}

func (cth *ChatHandle) LogErrorf(format string, options ...interface{}) {
	if cth == nil {
		errstr := "nil chat handle passed to LogErrorf"
		log.Errorf(errstr)
	} else {
		if cth.ErrorChannel == nil {
			log.Errorf("No ErrorChannel target defined on %s!\n", cth.Identifier())
		} else {
			cth.ErrorChannel.Sendf(format, options...)
		}
	}
	log.Printf("(Following message also sent to chat channel '%s')\n", cth.ErrorChannel.Name)
	log.Errorf(format, options...)
}

func (cth *ChatHandle) SendChannelf(Channel string, format string, options ...interface{}) (*ChatUpdateHandle, error) {
	if cth == nil {
		errstr := "nil chat handle passed to SendChannelf"
		log.Errorf(errstr)
		return nil, fmt.Errorf(errstr)
	}
	return cth.SendOut(Channel, fmt.Sprintf(format, options...))
}

func (cth *ChatHandle) SendDefaultf(format string, options ...interface{}) (*ChatUpdateHandle, error) {
	if cth == nil {
		errstr := "nil chat handle passed to SendDefaultf"
		log.Errorf(errstr)
		return nil, fmt.Errorf(errstr)
	}
	DieIfNil(cth.FpSendSimple == nil, "FpSendSimple is nil.\n")
	return cth.FpSendSimple(cth, cth.OutputChannel, fmt.Sprintf(format, options...))
}

func (cth *ChatHandle) NewMessage() *ChatMessage {
	Msg := ChatMessage{
		handle:  cth,
		Sender:  cth.Sender,
		Message: "",
		Target:  cth.OutputChannel,
		MsgType: MsgPostOrUpdate,
	}
	return &Msg
}

func (Msg *ChatMessage) Add2Columns(Left string, Right string) {
	Msg.Table = append(Msg.Table,
		[]string{
			Left, Right,
		})
}

func init() {
	ChatHandleConfigs = make(map[string]*ChatTypeConfig)
}

func GetLogger() *slogger.Logger {
	return log
}
