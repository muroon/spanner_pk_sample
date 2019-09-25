package spncloudfunctions

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/spanner"

	"github.com/muroon/spanner_pk_sample/spn"
)

// DefaultNumber insert Number
const DefaultNumber int = 10

// incrementNum number of auto increment
var incrementNum int64

// client is a global Spanner client, to avoid initializing a new client for
// every request.
var client *spanner.Client

type publishRequest struct {
	Mode     string `json:"mode"`
	TestMode string `json:"testmode"`
	Num      int    `json:"num"`
	Delete   bool   `json:"delete"`
}

// InsertSpanner is an example of querying Spanner from a Cloud Function.
func InsertSpanner(w http.ResponseWriter, r *http.Request) {
	err := executeInsert(w, r)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error %#v", err), http.StatusInternalServerError)
		log.Printf("iter.Next: %v", err)
		return
	}
}

func executeInsert(
	w http.ResponseWriter, r *http.Request,
) error {
	ctx := r.Context()

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("ioutil.ReadAll: %v", err)
		http.Error(w, "Error reading request", http.StatusBadRequest)
		return nil
	}

	p := publishRequest{}
	if err := json.Unmarshal(data, &p); err != nil {
		log.Printf("json.Unmarshal: %v", err)
		http.Error(w, "Error parsing request", http.StatusBadRequest)
		return nil
	}

	md := spn.Mode(p.Mode)
	tmd := spn.TestMode(p.TestMode)
	num := p.Num
	delete := p.Delete
	fmt.Fprintf(w, "Mode:%v, num:%d\n", md, num)

	spnm := spn.NewSpannerManager(
		spn.SetProjectID(os.Getenv("GCP_PROJECT")),
		spn.SetInstanceID(os.Getenv("SPN_INSTANCE_ID")),
		spn.SetDatabaseID(os.Getenv("SPN_DATABASE_ID")),
	)

	return spnm.ExecuteInsert(
		ctx, md, tmd, num, delete,
	)
}
