package shared

import (
	"fmt"
	"io/ioutil"
	"os/exec"
)

type MailMessage struct {
	handle  *MailHandle
	From    string
	To      string
	Subject string
	Body    string
}

type MailHandle struct {
	DefaultSender    string
	DefaultRecipient string
	SubjectPrefix    string
	SendmailPath     string
	ActuallySend     bool
}

func (Caw *MailHandle) NewMailMessage() *MailMessage {
	var Msg MailMessage
	Msg.handle = Caw
	Msg.From = Caw.DefaultSender
	Msg.To = Caw.DefaultRecipient
	Msg.Subject = Caw.SubjectPrefix
	return &Msg
}

func (Msg *MailMessage) SendViaSendmail() string {
	if !Msg.handle.ActuallySend {
		log.Printf("PRINT-ONLY MAIL MESSAGE:\nFrom: %s\nTo: %s\nSubject: %s%s\n\n%s\n",
			Msg.From, Msg.To, Msg.handle.SubjectPrefix, Msg.Subject, Msg.Body)
		return "DID NOT SEND - print-only mode"
	}
	var SendmailPath = "/usr/sbin/sendmail"
	if Msg.handle.SendmailPath != "" {
		SendmailPath = Msg.handle.SendmailPath
	}
	var msg string
	if Msg.From != "" {
		msg += fmt.Sprintf("From: %s\n", Msg.From)
	}
	if Msg.To != "" {
		msg += fmt.Sprintf("To: %s\n", Msg.To)
	}
	msg += fmt.Sprintf("Subject: %s%s\n\n%s", Msg.handle.SubjectPrefix, Msg.Subject, Msg.Body)

	log.Printf("Sendmail debugging: sendmail body is:\n%s\n\n====\n", msg)
	sendmail := exec.Command(SendmailPath, "-t")
	stdin, err := sendmail.StdinPipe()
	if err != nil {
		panic(err)
	}
	stdout, err := sendmail.StdoutPipe()
	if err != nil {
		panic(err)
	}
	err = sendmail.Start()
	if err != nil {
		log.Printf("ERROR on sendmail start: '%s'\n", err)
	}
	stdin.Write([]byte(msg))
	stdin.Close()
	sentBytes, _ := ioutil.ReadAll(stdout)
	sendmail.Wait()

	return fmt.Sprintf("sendmail Command Output: %s\n", string(sentBytes))
}
