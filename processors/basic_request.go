package processors

import (
	"crypto/tls"
	"encoding/base64"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"

	"github.com/bradstimpson/pipes/data"
	"github.com/bradstimpson/pipes/util"
)

// A Custom HTTPrequest built for basic authentication

func httpClientBuilder() http.Client {
	cookieJar, _ := cookiejar.New(nil)
	transporter := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	cli := &http.Client{
		Transport: transporter,
		Jar:       cookieJar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}}

	return *cli
}

// add basic authentication
func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

type JHTTPRequest struct {
	Request *http.Request
	Client  *http.Client
}

// a small example to work with the jira API
//func JiraHTTPRequest(method, url string, body io.Reader, cuser string, cpass string) (*JHTTPRequest, error) {
//	req, err := http.NewRequest(method, url, body)
// favored client
//	cli := httpClientBuilder()
//	req.Header.Add("Authorization", "Basic "+basicAuth(cuser, cpass))
//	return &JHTTPRequest{Request: req, Client: &cli}, err
//}

// Basic Auth request

func PipesHTTPBasicRequest(method, url string, cuser string, cpass string) (*http.Response, error) {
	//req, err := http.Get(url)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Add("Authorization", "Basic "+basicAuth(cuser, cpass))
	cli := httpClientBuilder()
	get, _ := cli.Do(req)
	return get, err
}

// X-API request
func PipeXAPIHTTPRequest(method, url string, body io.Reader, ckey string) (*JHTTPRequest, error) {
	req, err := http.NewRequest(method, url, body)
	// favored client
	cli := httpClientBuilder()
	req.Header.Add("X-Api-Key", ckey)
	return &JHTTPRequest{Request: req, Client: &cli}, err
}

// ProcessData sends data to outputChan if the response body is not null
func (r *JHTTPRequest) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	resp, err := r.Client.Do(r.Request)
	util.KillPipelineIfErr(err, killChan)
	if resp != nil && resp.Body != nil {
		dd, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		util.KillPipelineIfErr(err, killChan)
		outputChan <- dd
	}
}

// Finish - see interface for documentation.
func (r *JHTTPRequest) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *JHTTPRequest) String() string {
	return "HTTPRequest"
}
