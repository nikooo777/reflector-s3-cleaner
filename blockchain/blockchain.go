package blockchain

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/sirupsen/logrus"
)

type HubResponse struct {
	Jsonrpc string `json:"jsonrpc"`
	Result  string `json:"result"`
	Id      int    `json:"id"`
}

func ClaimExists(claimID string) (bool, error) {
	request := fmt.Sprintf(`{ "id": 0, "method":"blockchain.claimtrie.getclaimbyid", "params": ["%s"]}`+"\n", claimID)

	// Connect to the server
	conn, err := net.Dial("tcp", "s-hub1.odysee.com:50001")
	if err != nil {
		logrus.Println("Error connecting:", err.Error())
		return false, errors.Err(err)
	}
	defer conn.Close()

	// Write the request
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second)) // Set timeout
	_, err = conn.Write([]byte(request))
	if err != nil {
		logrus.Println("Error writing:", err.Error())
		return false, errors.Err(err)
	}

	// Read the response
	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) // Set timeout
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		logrus.Println("Error reading:", err.Error())
		return false, errors.Err(err)
	}

	var hubResponse HubResponse
	err = json.Unmarshal([]byte(response), &hubResponse)
	if err != nil {
		return false, errors.Err(err)
	}
	//base64 decode the result
	decoded, err := base64.StdEncoding.DecodeString(hubResponse.Result)
	if err != nil {
		return false, errors.Err(err)
	}
	stringRepresentation := string(decoded)
	if strings.Contains(stringRepresentation, "Could not find claim at") {
		return false, nil
	}

	return true, nil
}
