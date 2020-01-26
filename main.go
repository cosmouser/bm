package main

import (
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/youtube/v3"
)

// Flags
var (
	clientID     = flag.String("clientid", "", "OAuth 2.0 Client ID.  If non-empty, overrides --clientid_file")
	clientIDFile = flag.String("clientid-file", "clientid.dat",
		"Name of a file containing just the project's OAuth 2.0 Client ID from https://developers.google.com/console.")
	secret     = flag.String("secret", "", "OAuth 2.0 Client Secret.  If non-empty, overrides --secret_file")
	secretFile = flag.String("secret-file", "clientsecret.dat",
		"Name of a file containing just the project's OAuth 2.0 Client Secret from https://developers.google.com/console.")
	streamIDFile      = flag.String("streamid-file", "streamid.dat", "Name of a file containing just the YouTube LiveStream ID to use.")
	broadcastName     = flag.String("broadcastname", "", "Name to use for the broadcasts. If non-empty, overrides --broadcastname-file")
	broadcastNameFile = flag.String("broadcastname-file", "broadcastname.dat", "Name of a file containing just the name to use for the broadcasts")
	rtspURIFile       = flag.String("rtspuri-file", "rtspuri.dat", "Name of a file containing just the name to use for the broadcasts")
	dbPath            = flag.String("db", "bm.db", "File path to persistent store location. The store holds the current stream information")
	cacheToken        = flag.Bool("cachetoken", true, "cache the OAuth 2.0 token")
	debug             = flag.Bool("debug", false, "show HTTP traffic")
)

func main() {
	flag.Parse()

	config := &oauth2.Config{
		ClientID:     valueOrFileContents(*clientID, *clientIDFile),
		ClientSecret: valueOrFileContents(*secret, *secretFile),
		Endpoint:     google.Endpoint,
		Scopes:       []string{youtube.YoutubeScope, youtube.YoutubeReadonlyScope, youtube.YoutubeUploadScope, youtube.YoutubepartnerScope, youtube.YoutubepartnerChannelAuditScope},
	}

	ctx := context.Background()
	c := newOAuthClient(ctx, config)
	youtubeMain(c)
}
func youtubeMain(client *http.Client) {

	service, err := youtube.New(client)
	if err != nil {
		log.Fatalf("Unable to create YouTube service: %v", err)
	}

	// Test 1 - Create broadcast, bind livestream, transition to live
	// then, transition broadcastStatus to complete
	// if contentDetails.enableArchive and contentDetails.enableDvr were enabled
	// the video will be saved.
	liveBroadcastService := youtube.NewLiveBroadcastsService(service)
	broadcastInput := &youtube.LiveBroadcast{
		ContentDetails: &youtube.LiveBroadcastContentDetails{
			EnableDvr:       true,
			RecordFromStart: true,
		},
		Status: &youtube.LiveBroadcastStatus{
			LifeCycleStatus:         "created",
			MadeForKids:             false,
			PrivacyStatus:           "private",
			RecordingStatus:         "notRecording",
			SelfDeclaredMadeForKids: false,
		},
		Snippet: &youtube.LiveBroadcastSnippet{
			Title:              valueOrFileContents(*broadcastName, *broadcastNameFile) + " | " + time.Now().Format(time.RubyDate),
			Description:        "Live stream by BroadcastManager",
			ScheduledStartTime: time.Now().Format(time.RFC3339),
		},
	}

	// INSERT
	insertCall := liveBroadcastService.Insert("snippet,contentDetails,status", broadcastInput)
	liveBroadcast, err := insertCall.Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}
	log.Printf("after INSERT LiveBroadcast: %+v LiveBroadcastStatus: %+v", liveBroadcast, liveBroadcast.Status)

	// BIND
	bindCall := liveBroadcastService.Bind(liveBroadcast.Id, "snippet,contentDetails,status")
	liveBroadcast, err = bindCall.StreamId(valueOrFileContents("", *streamIDFile)).Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}
	log.Printf("after BIND LiveBroadcast: %+v LiveBroadcastStatus: %+v", liveBroadcast, liveBroadcast.Status)

	// TRANSITION TO TESTING
	transitionCall := liveBroadcastService.Transition("testing", liveBroadcast.Id, "snippet,status")
	liveBroadcast, err = transitionCall.Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}
	log.Printf("after TRANSITION LiveBroadcast: %+v LiveBroadcastStatus: %+v", liveBroadcast, liveBroadcast.Status)

	// Wait for transition to complete before transitioning to live
	// This works well enough for now. Eventually we'll want to change this to poll for state in order to operate deterministically.
	time.Sleep(time.Second * 15)

	// TRANSITION TO LIVE
	transitionCall = liveBroadcastService.Transition("live", liveBroadcast.Id, "snippet,status")
	liveBroadcast, err = transitionCall.Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}
	log.Printf("after TRANSITION LiveBroadcast: %+v LiveBroadcastStatus: %+v", liveBroadcast, liveBroadcast.Status)
}
func valueOrFileContents(value string, filename string) string {
	if value != "" {
		return value
	}
	slurp, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error reading %q: %v", filename, err)
	}
	return strings.TrimSpace(string(slurp))
}

func newOAuthClient(ctx context.Context, config *oauth2.Config) *http.Client {
	cacheFile := tokenCacheFile(config)
	token, err := tokenFromFile(cacheFile)
	if err != nil {
		token = tokenFromWeb(ctx, config)
		saveToken(cacheFile, token)
	} else {
		log.Printf("Using cached token %#v from %q", token, cacheFile)
	}

	return config.Client(ctx, token)
}

func tokenCacheFile(config *oauth2.Config) string {
	hash := fnv.New32a()
	hash.Write([]byte(config.ClientID))
	hash.Write([]byte(config.ClientSecret))
	hash.Write([]byte(strings.Join(config.Scopes, " ")))
	fn := fmt.Sprintf("go-api-demo-tok%v", hash.Sum32())
	return filepath.Join(osUserCacheDir(), url.QueryEscape(fn))
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	if !*cacheToken {
		return nil, errors.New("--cachetoken is false")
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	t := new(oauth2.Token)
	err = gob.NewDecoder(f).Decode(t)
	return t, err
}

func saveToken(file string, token *oauth2.Token) {
	f, err := os.Create(file)
	if err != nil {
		log.Printf("Warning: failed to cache oauth token: %v", err)
		return
	}
	defer f.Close()
	gob.NewEncoder(f).Encode(token)
}

func tokenFromWeb(ctx context.Context, config *oauth2.Config) *oauth2.Token {
	ch := make(chan string)
	randState := fmt.Sprintf("st%d", time.Now().UnixNano())
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/favicon.ico" {
			http.Error(rw, "", 404)
			return
		}
		if req.FormValue("state") != randState {
			log.Printf("State doesn't match: req = %#v", req)
			http.Error(rw, "", 500)
			return
		}
		if code := req.FormValue("code"); code != "" {
			fmt.Fprintf(rw, "<h1>Success</h1>Authorized.")
			rw.(http.Flusher).Flush()
			ch <- code
			return
		}
		log.Printf("no code")
		http.Error(rw, "", 500)
	}))
	defer ts.Close()

	config.RedirectURL = ts.URL
	authURL := config.AuthCodeURL(randState)
	log.Printf("Authorize this app at: %s", authURL)
	code := <-ch
	log.Printf("Got code: %s", code)

	token, err := config.Exchange(ctx, code)
	if err != nil {
		log.Fatalf("Token exchange error: %v", err)
	}
	return token
}

func osUserCacheDir() string {
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Caches")
	case "linux", "freebsd":
		return filepath.Join(os.Getenv("HOME"), ".cache")
	}
	log.Printf("TODO: osUserCacheDir on GOOS %q", runtime.GOOS)
	return "."
}
