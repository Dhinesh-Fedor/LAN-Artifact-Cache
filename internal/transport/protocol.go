// Wire format for artifact fetch requests.
package transport

import "encoding/json"

type Request struct {
	Hash string `json:"hash"`
}

type Response struct {
	OK    bool   `json:"ok"`
	Hash  string `json:"hash,omitempty"`
	Size  int64  `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
}

func EncodeRequest(r Request) ([]byte, error) {
	return json.Marshal(r)
}

func EncodeResponse(r Response) ([]byte, error) {
	return json.Marshal(r)
}
