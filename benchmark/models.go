package main

import (
	"encoding/json"
	"fmt"
	"time"
)

type TestStruct struct {
	ID      int
	Name    string
	Value   float64
	Active  bool
	Tags    []string
	Data    map[string]int
	Created time.Time
	Updated time.Time
	Score   int
	Note    string
}

type MableEvent struct {
	BD        BrowserData            `json:"bd"`
	CDAS      CustomerData           `json:"cdas"`
	EID       string                 `json:"eid"`
	EN        string                 `json:"en"`
	II        string                 `json:"ii"`
	MD        Metadata               `json:"md"`
	PD        PageData               `json:"pd"`
	PIDS      PIDs                   `json:"pids"`
	SD        SessionData            `json:"sd"`
	TS        int64                  `json:"ts"`
	UAHeaders map[string]interface{} `json:"uaHeaders"`
	ESD       map[string]interface{} `json:"esd"`
}

type BrowserData struct {
	AID       string                 `json:"aid"`
	BSTS      int64                  `json:"bsts"`
	CKS       map[string]string      `json:"cks"`
	IQPAR     map[string]interface{} `json:"iqpar"`
	IR        string                 `json:"ir"`
	IsBackend bool                   `json:"is_backend"`
	SRH       int                    `json:"srh"`
	SRW       int                    `json:"srw"`
	UA        string                 `json:"ua"`
	UL        string                 `json:"ul"`
}

type CustomerData struct {
	CC     []string      `json:"cc"`
	CT     []string      `json:"ct"`
	DOB    []interface{} `json:"dob"`
	DS     []string      `json:"ds"`
	EM     []string      `json:"em"`
	FN     []string      `json:"fn"`
	GE     []interface{} `json:"ge"`
	ID     []string      `json:"id"`
	LN     []string      `json:"ln"`
	PH     []interface{} `json:"ph"`
	RC     []interface{} `json:"rc"`
	RG     []interface{} `json:"rg"`
	Street []string      `json:"street"`
	ZIP    []string      `json:"zip"`
}

type SVCMetric struct {
	EAT string `json:"eat"`
	EDT string `json:"edt"`
	SN  string `json:"sn"`
}

type Metadata struct {
	DUIDS        []string          `json:"duids"`
	EO           string            `json:"eo"`
	PV           string            `json:"pv"`
	SessionFound bool              `json:"session_found"`
	SUID         string            `json:"suid"`
	SYST         string            `json:"syst"`
	SVCMetrics   []SVCMetric       `json:"svc_metrics"`
	DUIDConvMap  map[string]string `json:"duid_conv_map"`
}

type PageData struct {
	DL   string                 `json:"dl"`
	DT   string                 `json:"dt"`
	MP   int                    `json:"mp"`
	PB   int64                  `json:"pb"`
	PHCT int                    `json:"phct"`
	PP   float64                `json:"pp"`
	QPAR map[string]interface{} `json:"qpar"`
	RL   string                 `json:"rl"`
}

type PIDs struct {
	EPIK   []interface{} `json:"epik"`
	FBCLID []interface{} `json:"fbclid"`
	GACID  []interface{} `json:"gacid"`
	GCLID  []interface{} `json:"gclid"`
	TTCLID []interface{} `json:"ttclid"`
}

type MerchantData struct {
	DO         string `json:"do"`
	LO         string `json:"lo"`
	MID        string `json:"mid"`
	RE         string `json:"re"`
	MH         string `json:"mh"`
	IsCaptured bool   `json:"is_captured"`
}

type SessionData struct {
	IP4       string                 `json:"ip4"`
	IP6       string                 `json:"ip6"`
	IQPAR     map[string]interface{} `json:"iqpar"`
	IsBackend bool                   `json:"is_backend"`
	MID       MerchantData           `json:"mid"`
	RL        string                 `json:"rl"`
	SCT       int                    `json:"sct"`
	SID       string                 `json:"sid"`
	SSTS      int64                  `json:"ssts"`
}

// ParseMableEventJSON parses a JSON byte slice into a MableEvent
func ParseMableEventJSON(data []byte) (*MableEvent, error) {
	var event MableEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal MableEvent JSON: %w", err)
	}
	return &event, nil
}
