package drill

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

const (
	contentTypeHeaderKey = "Content-Type"
	contentTypeJSON      = "application/json"
)

type Client struct {
	url    string
	client *http.Client
}

func NewClient(host string, port int, useSSL bool) *Client {
	client := &Client{}

	var scheme = ""
	if useSSL {
		scheme = "https"
	} else {
		scheme = "http"
	}

	client.url = fmt.Sprintf("%s://%s:%d", scheme, host, port)

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client.client = &http.Client{Transport: tr}

	return client
}

type RequestBody struct {
	QueryType string `json:"queryType"`
	Query     string `json:"query"`
}

type ResponseBody struct {
	ErrorMessage string   `json:"errorMessage"`
	QueryID      string   `json:"queryId"`
	Columns      []string `json:"columns"`
	Rows         []struct {
		Summary string `json:"summary"`
		Ok      string `json:"ok"`
	} `json:"rows"`
	Metadata           []string `json:"metadata"`
	QueryState         string   `json:"queryState"`
	AttemptedAutoLimit int      `json:"attemptedAutoLimit"`
}

func (c *Client) do(method string, path string, body RequestBody) (ResponseBody, error) {
	var respBody = ResponseBody{}
	var addr = fmt.Sprintf("%s/%s", c.url, path)
	b, err := json.Marshal(body)
	if err != nil {
		return respBody, newError(
			http.StatusBadRequest,
			method,
			addr,
			errors.Wrap(err, "unable to marshal upsert object").Error(),
		)
	}
	buf := bytes.NewBuffer(b)

	req, err := http.NewRequest(method, addr, buf)
	if err != nil {
		return respBody, newError(
			http.StatusBadRequest,
			method,
			addr,
			errors.Wrap(err, "unable to to create new request").Error(),
		)
	}

	req.Header.Set(contentTypeHeaderKey, contentTypeJSON)

	resp, err := c.client.Do(req)
	if err != nil {
		return respBody, newError(
			resp.StatusCode,
			method,
			addr,
			errors.Wrap(err, "unable to perform client request").Error(),
		)
	}
	defer resp.Body.Close()

	err = c.readJSON(resp, &respBody)

	if err != nil {
		return respBody, newError(
			http.StatusBadRequest,
			method,
			addr,
			errors.Wrap(err, "unable to read json response").Error(),
		)
	}

	if resp.StatusCode != http.StatusOK {
		return respBody, newError(
			resp.StatusCode,
			method,
			addr,
			errors.New("statuscode is not equal to 200").Error(),
		)
	}

	return respBody, nil
}

func (c *Client) post(path string, u RequestBody) (ResponseBody, error) {
	return c.do("POST", path, u)
}

func (c *Client) readJSON(resp *http.Response, valuePtr interface{}) error {
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, valuePtr)
}
