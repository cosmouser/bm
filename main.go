package main

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/boltdb/bolt"
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
	db                *bolt.DB
	storeBucket       = []byte("store")
	broadcastKey      = []byte("broadcast")
	endDuration       = time.Duration(time.Hour * 6)
)

type broadcastInfo struct {
	ID    string         `json:"id"`
	State broadcastState `json:"state"`
	Start time.Time      `json:"start"`
	Last  time.Time      `json:"last"`
}
type broadcastState int

const (
	BROADCAST_STARTING broadcastState = 0
	BROADCAST_LIVE     broadcastState = 1
	BROADCAST_ERROR    broadcastState = 2
)

func (b broadcastState) String() string {
	switch b {
	case 0:
		return "BROADCAST_STARTING"
	case 1:
		return "BROADCAST_LIVE"
	case 2:
		return "BROADCAST_ERROR"
	default:
		return "BROADCAST_UNKNOWN"
	}
}
func main() {
	flag.Parse()

	var err error
	db, err = bolt.Open(*dbPath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	initLocalStore()
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
	errCount := 0
	for !liveStreamHealthCheck(service) {
		if errCount < 4 {
			errCount++
		}
		time.Sleep(time.Duration(15*errCount) * time.Second)
	}
	state, err := findState(service)
	if err != nil {
		log.Fatalf("Error checking state: %v", err)
	}
	// If state is BROADCAST_STARTING, run steps to insert a new broadcast
	// If state is BROADCAST_LIVE, go to regular status polling
	// If state is BROADCAST_ERROR, stream is bad or unable to auth.
	// Attempt to end stream if local ID matches external ID, then change status
	// to BROADCAST_STARTING.
	// If unable to end the stream, keep trying at regular intervals.
	// With the application kept in the BROADCAST_ERROR state, prometheus
	// should be able to alert on a problem. We don't want to start a new stream
	// because we want to make sure the old one has been ended.
	switch state {
	case BROADCAST_STARTING:
		insertBroadcast(service)
		broadcastManager(service)
	case BROADCAST_LIVE:
		broadcastManager(service)
	case BROADCAST_ERROR:
		log.Warn("youtubeMain state is ERROR")
	default:
		log.Fatal("unexpected broadcast state")
	}
}

// broadcastManager checks the status of the current broadcast regularly to manage it
func broadcastManager(svc *youtube.Service) {
	c := time.Tick(15 * time.Second)
	for range c {
		state := findLocalState()
		switch state {
		case BROADCAST_STARTING:
			db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket(storeBucket)
				v := b.Get(broadcastKey)
				broadcast := broadcastInfo{}
				err := json.Unmarshal(v, &broadcast)
				if err != nil {
					return err
				}
				if time.Since(broadcast.Start) > 3*time.Minute {
					log.Warn("broadcast has been in state STARTING for over 3 minutes")
				}
				return nil
			})
		case BROADCAST_LIVE:
			shouldCheck := false
			shouldEnd := false
			db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket(storeBucket)
				v := b.Get(broadcastKey)
				broadcast := broadcastInfo{}
				err := json.Unmarshal(v, &broadcast)
				if err != nil {
					return err
				}
				if time.Since(broadcast.Last) > 2*time.Minute {
					shouldCheck = true
				}
				return nil
			})
			if shouldCheck {
				shouldEnd = checkEndCondition()
			}
			if shouldEnd {
				completeBroadcast(svc)
			}
		case BROADCAST_ERROR:
			log.Warn("youtubeMain state is ERROR")
		default:
			log.Fatal("unexpected broadcast state")
		}
	}
}
func completeBroadcast(svc *youtube.Service) {
	lbs := youtube.NewLiveBroadcastsService(svc)
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(storeBucket)
		v := b.Get(broadcastKey)
		broadcast := broadcastInfo{}
		err := json.Unmarshal(v, &broadcast)
		if err != nil {
			return err
		}
		transitionCall := lbs.Transition("complete", broadcast.ID, "snippet,status")
		_, err = transitionCall.Do()
		if err != nil {
			log.Fatalf("Error making YouTube API call: %v", err)
		}
		log.WithFields(log.Fields{
			"broadcastId": broadcast.ID,
			"ended":       time.Now(),
		}).Info("broadcast ended")
		broadcast.State = BROADCAST_STARTING
		serial, err := json.Marshal(broadcast)
		if err != nil {
			log.Fatal(err)
		}
		err = b.Put(broadcastKey, serial)
		return nil
	})
	time.Sleep(15 * time.Second)
	insertBroadcast(svc)
}

// checkEndCondition checks if the currentBroadcast has gone over the maximum duration
func checkEndCondition() bool {
	var result bool
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(storeBucket)
		v := b.Get(broadcastKey)
		if v == nil {
			log.Fatal("checking for end condition on non existent broadcast")
		}
		broadcast := broadcastInfo{}
		err := json.Unmarshal(v, &broadcast)
		if err != nil {
			return err
		}
		broadcast.Last = time.Now()
		serial, err := json.Marshal(broadcast)
		if err != nil {
			log.Fatal(err)
		}
		err = b.Put(broadcastKey, serial)
		if time.Since(broadcast.Start) > endDuration {
			result = true
		}
		return nil
	})
	return result
}

func liveStreamHealthCheck(svc *youtube.Service) bool {
	lss := youtube.NewLiveStreamsService(svc)
	lslc := lss.List("snippet,cdn,contentDetails,status")
	resp, err := lslc.Id(valueOrFileContents("", *streamIDFile)).Do()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Error checking liveStream health")
	}
	if len(resp.Items) == 0 {
		log.WithFields(log.Fields{
			"streamID": valueOrFileContents("", *streamIDFile),
		}).Fatal("Stream not found")
	}
	if len(resp.Items[0].Status.HealthStatus.ConfigurationIssues) != 0 {
		for _, v := range resp.Items[0].Status.HealthStatus.ConfigurationIssues {
			log.Error(v.Description)
		}
	}
	return true
}

// findState checks the internal store for the LiveBroadcast status.
// If the internal store matches YT's data then it resumes managing the current
// stream. If it does not match, or if there is no internal data, the result
// can be used to start a new broadcast.
func findState(svc *youtube.Service) (broadcastState, error) {
	state := BROADCAST_STARTING
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(storeBucket)
		v := b.Get(broadcastKey)
		// if v doesn't exist v will be nil
		// if nil, retain state as BROADCAST_STARTING
		if v == nil {
			return nil
		}
		broadcast := broadcastInfo{}
		err := json.Unmarshal(v, &broadcast)
		if err != nil {
			return err
		}
		if broadcast.ID == "" {
			return nil
		}
		lbs := youtube.NewLiveBroadcastsService(svc)
		lbslc := lbs.List("snippet,contentDetails,status")
		resp, err := lbslc.BroadcastStatus("active").Do()
		if err != nil {
			log.Error(err)
		}
		if len(resp.Items) == 0 {
			return nil
		}
		for _, v := range resp.Items {
			if v.Id == broadcast.ID {
				state = BROADCAST_LIVE
			} else {
				log.WithFields(log.Fields{
					"liveBroadcastId": v.Id,
				}).Info("Found unmanaged active broadcast")
			}
		}
		return nil
	})
	return state, err
}
func findLocalState() broadcastState {
	var state broadcastState
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(storeBucket)
		v := b.Get(broadcastKey)
		broadcast := broadcastInfo{}
		err := json.Unmarshal(v, &broadcast)
		if err != nil {
			return err
		}
		state = broadcast.State
		return nil
	})
	return state
}

// pollAndTransitionToLive polls the current LiveBroadcast for ready status, then
// calls for a transition to "live" status.
func pollForTransitionToLive(svc *youtube.LiveBroadcastsService) bool {
	var currentBroadcastID string
	lc := svc.List("snippet,contentDetails,status")
	resp, err := lc.BroadcastStatus("upcoming").Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}
	if len(resp.Items) == 0 {
		log.Warn("No upcoming liveBroadcasts found in list response")
		return false
	}
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(storeBucket)
		v := b.Get(broadcastKey)
		// if v doesn't exist v will be nil
		// if nil, retain state as BROADCAST_STARTING
		if v == nil {
			return nil
		}
		broadcast := broadcastInfo{}
		err := json.Unmarshal(v, &broadcast)
		if err != nil {
			return err
		}
		currentBroadcastID = broadcast.ID
		return nil
	})
	for _, v := range resp.Items {
		if v.Id == currentBroadcastID {
			if v.Status.LifeCycleStatus == "testing" {
				return true
			}
		}
	}
	return false
}

func insertBroadcast(svc *youtube.Service) {
	liveBroadcastService := youtube.NewLiveBroadcastsService(svc)
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
			Title:              valueOrFileContents(*broadcastName, *broadcastNameFile) + " | " + time.Now().Format(time.RFC3339),
			Description:        "Live stream by BroadcastManager",
			ScheduledStartTime: time.Now().Format(time.RFC3339),
		},
	}
	insertCall := liveBroadcastService.Insert("snippet,contentDetails,status", broadcastInput)
	liveBroadcast, err := insertCall.Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(storeBucket)
		dat := broadcastInfo{
			ID:    liveBroadcast.Id,
			State: BROADCAST_STARTING,
			Start: time.Now(),
			Last:  time.Now(),
		}
		serial, err := json.Marshal(dat)
		if err != nil {
			log.Fatal(err)
		}
		err = b.Put(broadcastKey, serial)
		return err
	})

	bindCall := liveBroadcastService.Bind(liveBroadcast.Id, "snippet,contentDetails,status")
	liveBroadcast, err = bindCall.StreamId(valueOrFileContents("", *streamIDFile)).Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}

	// Call for a transition to testing.
	transitionCall := liveBroadcastService.Transition("testing", liveBroadcast.Id, "snippet,status")
	liveBroadcast, err = transitionCall.Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}

	// Wait for transition to complete before transitioning to live.
	for !pollForTransitionToLive(liveBroadcastService) {
		time.Sleep(5 * time.Second)
	}

	// Call for a transition to live.
	transitionCall = liveBroadcastService.Transition("live", liveBroadcast.Id, "snippet,status")
	liveBroadcast, err = transitionCall.Do()
	if err != nil {
		log.Fatalf("Error making YouTube API call: %v", err)
	}
	started := time.Now()
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(storeBucket)
		dat := broadcastInfo{
			ID:    liveBroadcast.Id,
			State: BROADCAST_LIVE,
			Start: started,
		}
		serial, err := json.Marshal(dat)
		if err != nil {
			log.Fatal(err)
		}
		err = b.Put(broadcastKey, serial)
		return err
	})
	log.WithFields(log.Fields{
		"broadcastId": liveBroadcast.Id,
		"started":     started,
	}).Info("New broadcast started")
}
func initLocalStore() {
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("store"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
			os.Exit(1)
		}
		return nil
	})
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
