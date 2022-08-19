package simage

import (
	"fmt"
	"github.com/grammaton76/g76golib/slogger"
	"io/ioutil"
	"net/http"
)

var log *slogger.Logger

type Image struct {
	provenance string
	fetchUrl   string
	client     *http.Client
	filename   string
	content    []byte
	fetched    bool
	err        error
}

func SetLogger(l *slogger.Logger) *slogger.Logger {
	log = l
	return l
}

func (img *Image) Identifier() string {
	return img.provenance
}

func (img *Image) Content() ([]byte, error) {
	if img.fetched {
		return img.content, img.err
	}
	if img.filename != "" {
		img.content, img.err = ioutil.ReadFile(img.filename)
		img.fetched = true
		return img.content, img.err
	}
	if img.fetchUrl != "" {
		img.fetched = true
		if img.fetchUrl == "" {
			return nil, fmt.Errorf("%s had blank url", img.Identifier())
		}
		Client := img.client
		if Client == nil {
			Client = &http.Client{}
		}
		var resp *http.Response
		resp, img.err = Client.Get(img.fetchUrl)
		if img.err != nil {
			return nil, img.err
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return nil, fmt.Errorf("HTTP-%d response on GET %s", resp.StatusCode, img.fetchUrl)
		}
		img.content, img.err = ioutil.ReadAll(resp.Body)
		return img.content, img.err
	}
	return nil, fmt.Errorf("unknown way to fetch content for %s", img.Identifier())
}

func NewImageFromUrl(client *http.Client, Url string) (*Image, error) {
	Img := &Image{provenance: Url, fetchUrl: Url, client: client}
	return Img, nil
}
